// Package cli — the `dump` verb (the dump-restore-spec record §6).
//
// `hoarddb dump [database [bucket]] [flags]` is both a standalone verb
// (cmd/cli/main.go dispatches to RunDumpCommand before Connect/Run, since
// Run is the REPL and would otherwise hang a pipeline reading from stdin —
// dump-restore-build.md trap #9) and a REPL statement sharing the exact same
// grammar and the same runDumpBuckets implementation.
package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The exit codes of the dump and restore verbs are the shared table in
// exitcodes.go (dump-restore-spec.md §6). The REPL form does not exit the
// process; it reports failure through the ordinary "Error: ..." path instead.

// errDumpInterrupted marks a bucket dump abandoned because Ctrl-C fired.
var errDumpInterrupted = errors.New("interrupted")

// dumpBucketTarget names one bucket to dump.
type dumpBucketTarget struct {
	Database string
	Bucket   string
}

// dumpFailure records one bucket's failure for the top-level manifest's
// failure list (dump-restore-spec.md §4, §16.4).
type dumpFailure struct {
	Database string `json:"database"`
	Bucket   string `json:"bucket"`
	Error    string `json:"error"`
}

// bucketMetadataFile is the on-disk shape of <bucket>.metadata.json
// (dump-restore-spec.md §4). Bytes and Digest are computed here, by the
// CLI, from what was actually written to the .bson file — the server's
// DumpBucketManifest deliberately omits them (see server/dump.go).
type bucketMetadataFile struct {
	FormatVersion  int      `json:"format_version"`
	HoardDBVersion string   `json:"hoarddb_version"`
	ProtocolMajor  uint8    `json:"protocol_major"`
	CreatedAt      string   `json:"created_at"`
	SourceNode     string   `json:"source_node"`
	Database       string   `json:"database"`
	Bucket         string   `json:"bucket"`
	StoreType      string   `json:"store_type"`
	Indexes        []string `json:"indexes,omitempty"`
	// IndexSpecs is the bucket's index declaration, in id order.
	IndexSpecs    []server.DumpIndexSpec `json:"index_specs,omitempty"`
	Documents     int64                  `json:"documents"`
	Bytes         int64                  `json:"bytes"`
	Digest        string                 `json:"digest"`
	BlobsIncluded bool                   `json:"blobs_included"`
	// Stores, Elements, StoresBytes and StoresDigest describe a bucket's
	// store fields, when it declares any, and the <bucket>.stores.bson file
	// holding their element records, digested like the .bson file.
	Stores       []server.DumpStore `json:"stores,omitempty"`
	Elements     int64              `json:"elements,omitempty"`
	StoresBytes  int64              `json:"stores_bytes,omitempty"`
	StoresDigest string             `json:"stores_digest,omitempty"`
	// Dead, DeadBytes and DeadDigest describe the dead entries of a queue or a
	// heap (format 3) and the <bucket>.dead.bson file holding them, digested
	// like the .bson file. Absent for a bucket with none.
	Dead       int64  `json:"dead,omitempty"`
	DeadBytes  int64  `json:"dead_bytes,omitempty"`
	DeadDigest string `json:"dead_digest,omitempty"`
	// ReplicationFactor is the factor the bucket declared, absent for one
	// that inherits the node's; a restore re-creates the bucket with it.
	ReplicationFactor int `json:"replication_factor,omitempty"`
	// Placement is a hash bucket's recorded placement, spread or whole; a
	// restore creates the bucket with it.
	Placement string `json:"placement,omitempty"`
}

// topLevelManifest is the on-disk shape of manifest.json.
type topLevelManifest struct {
	FormatVersion       int           `json:"format_version"`
	HoardDBVersion      string        `json:"hoarddb_version"`
	CreatedAt           string        `json:"created_at"`
	SourceNode          string        `json:"source_node"`
	Databases           []string      `json:"databases"`
	Buckets             int           `json:"buckets"`
	Documents           int64         `json:"documents"`
	Bytes               int64         `json:"bytes"`
	BlobsIncluded       bool          `json:"blobs_included"`
	CredentialsIncluded bool          `json:"credentials_included"`
	GloballyConsistent  bool          `json:"globally_consistent"`
	Incomplete          bool          `json:"incomplete"`
	Failures            []dumpFailure `json:"failures,omitempty"`
}

// dumpResult is the single JSON object --json prints to stdout
// (dump-restore-spec.md §6): "{ok, databases, buckets, documents,
// users_recreated, users_awaiting_password, bytes, errors[]}". Users are not
// yet implemented (a later commit), so those two fields always report zero.
type dumpResult struct {
	OK                    bool          `json:"ok"`
	Databases             []string      `json:"databases"`
	Buckets               int           `json:"buckets"`
	Documents             int64         `json:"documents"`
	UsersRecreated        int           `json:"users_recreated"`
	UsersAwaitingPassword int           `json:"users_awaiting_password"`
	Bytes                 int64         `json:"bytes"`
	Warnings              []string      `json:"warnings,omitempty"`
	Errors                []dumpFailure `json:"errors"`
}

// nonNil returns an empty slice for a nil one, because encoding/json prints a
// nil slice as null and a script iterating --json's lists needs [].
func nonNil[T any](list []T) []T {
	if list == nil {
		return []T{}
	}
	return list
}

// dumpSummary accumulates the outcome of every bucket a dump run attempted.
type dumpSummary struct {
	sourceNode string
	databases  []string
	seenDBs    map[string]bool
	buckets    int
	documents  int64
	bytes      int64
	failures   []dumpFailure
	// warnings must reach both the text summary and --json. A dump that
	// silently omits data is the failure mode this list exists for: the
	// blob-payload notice below is the first entry it carries, mirroring
	// restoreSummary.warnings.
	warnings    []string
	incomplete  bool
	interrupted bool
}

func newDumpSummary() *dumpSummary {
	return &dumpSummary{seenDBs: make(map[string]bool)}
}

func (sm *dumpSummary) addDatabase(name string) {
	if !sm.seenDBs[name] {
		sm.seenDBs[name] = true
		sm.databases = append(sm.databases, name)
	}
}

// exitCode maps a summary to one of the distinct exit codes
// dump-restore-spec.md §6 requires: 0 clean, 3 partial (at least one bucket
// succeeded and at least one failed), 2 refused (nothing succeeded and
// something failed), 4 interrupted.
func (sm *dumpSummary) exitCode() int {
	switch {
	case sm.interrupted:
		return ExitInterrupted
	case len(sm.failures) == 0:
		return ExitOK
	case sm.buckets == 0:
		return ExitRefused
	default:
		return ExitPartial
	}
}

// RunDumpCommand implements the standalone `hoarddb dump` verb. It parses
// its own connection flags (the same ones cmd/cli/main.go's REPL path
// accepts) plus the dump-specific flags, connects, runs the dump, prints
// either a text summary or (with --json) exactly one JSON object to stdout,
// and returns the process exit code — the caller's job is `os.Exit(code)`.
func RunDumpCommand(args []string) int {
	installClientLogger(false)
	fs := flag.NewFlagSet("hoarddb dump", flag.ContinueOnError)
	var usage strings.Builder
	fs.SetOutput(&usage)

	connection := registerConnectionFlags(fs)
	out := fs.String("out", "./dump", "destination directory")
	dbFlag := fs.String("db", "", "database (alias for the first positional argument)")
	bucketFlag := fs.String("bucket", "", "bucket (alias for the second positional argument)")
	users := fs.Bool("users", true, "include users.json (not yet implemented; accepted for forward compatibility)")
	noUsers := fs.Bool("no-users", false, "skip users.json")
	jsonOut := fs.Bool("json", false, "print one JSON result object to stdout instead of a text summary")

	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "usage: hoarddb dump [database [bucket]] [flags]\n%s\n", usage.String())
		return ExitUsage
	}
	_ = users
	_ = noUsers

	database, bucket, err := dumpPositionalScope(positional, *dbFlag, *bucketFlag)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "usage: %v\n", err)
		return ExitUsage
	}

	// Progress and any incidental connection chatter go to stderr, never
	// stdout (dump-restore-spec.md §6): --json's caller pipes stdout to a
	// parser.
	spec := connection.spec(flagWasProvided(fs, "address"))
	spec.Chatter = os.Stderr
	c, err := connectSession(spec)
	if err != nil {
		return verbConnectFailure(os.Stderr, c, err)
	}
	defer c.Close()
	c.notef("Connected to %s\n", c.addr)

	interrupted := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	go func() {
		if _, ok := <-sig; ok {
			close(interrupted)
		}
	}()

	summary, runErr := runDumpBuckets(c.handler, database, bucket, *out, os.Stderr, interrupted)
	if runErr != nil {
		_, _ = fmt.Fprintf(os.Stderr, "dump: %s\n", terminalSafeText(runErr.Error()))
		// An --out the CLI cannot use is the caller's input (1); a database or
		// bucket the server says is absent is a refusal (2).
		var outDirProblem *UsageError
		if errors.As(runErr, &outDirProblem) {
			return ExitUsage
		}
		return ExitRefused
	}

	code := summary.exitCode()
	if *jsonOut {
		result := dumpResult{
			OK:        code == ExitOK,
			Databases: nonNil(summary.databases),
			Buckets:   summary.buckets,
			Documents: summary.documents,
			Bytes:     summary.bytes,
			Warnings:  summary.warnings,
			Errors:    nonNil(summary.failures),
		}
		payload, _ := json.Marshal(result)
		_, _ = fmt.Fprintln(os.Stdout, string(payload))
	} else {
		printDumpSummary(os.Stderr, summary)
	}
	return code
}

// parseFlagsAnywhere parses fs's flags wherever they stand and returns the
// positional words in order. The flag package stops at the first non-flag, so
// `dump app --out ./d` would leave "--out" and "./d" as positionals; the usage
// line `dump [database [bucket]] [flags]` promises the flags may come last.
// Everything after a "--" is positional, as the flag package defines it.
func parseFlagsAnywhere(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		consumed := len(args) - len(rest)
		if consumed > 0 && args[consumed-1] == "--" {
			return append(positional, rest...), nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

// dumpPositionalScope reconciles the positional [database [bucket]] form
// with the --db/--bucket aliases (dump-restore-spec.md §6): each may be
// given once, either way, not both.
func dumpPositionalScope(positional []string, dbFlag, bucketFlag string) (database, bucket string, err error) {
	database, bucket = dbFlag, bucketFlag
	if len(positional) > 2 {
		return "", "", fmt.Errorf("hoarddb dump [database [bucket]] [flags] — too many positional arguments")
	}
	if len(positional) > 0 {
		if database != "" {
			return "", "", fmt.Errorf("database given both positionally (%q) and with --db (%q)", positional[0], database)
		}
		database = positional[0]
	}
	if len(positional) > 1 {
		if bucket != "" {
			return "", "", fmt.Errorf("bucket given both positionally (%q) and with --bucket (%q)", positional[1], bucket)
		}
		bucket = positional[1]
	}
	if bucket != "" && database == "" {
		return "", "", fmt.Errorf("a bucket requires a database — hoarddb dump <database> <bucket>")
	}
	return database, bucket, nil
}

// legacyReplDumpNote follows every REPL dump summary: the statement writes the legacy
// engine's format, which the ring 'hoarddb restore' cannot read (ring-cli-parity-build.md
// section 5). It goes away with the legacy REPL.
const legacyReplDumpNote = "This is the file of the dump statement. 'hoarddb restore' reads only files from 'hoarddb dump'; they are different formats."

// handleDump is the REPL entry point: `dump [database [bucket]] [flags]` as
// a statement inside an interactive session, sharing runDumpBuckets with the
// standalone verb (dump-restore-spec.md §6: "Both forms share one
// implementation").
func (h *CommandHandler) handleDump(args []string) error {
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	out := fs.String("out", "./dump", "destination directory")
	dbFlag := fs.String("db", "", "database")
	bucketFlag := fs.String("bucket", "", "bucket")
	fs.Bool("users", true, "include users.json (not yet implemented)")
	fs.Bool("no-users", false, "skip users.json")
	fs.Bool("json", false, "print a JSON result instead of a text summary")
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return fmt.Errorf("usage: dump [database [bucket]] [--out <dir>]: %w", err)
	}
	database, bucket, err := dumpPositionalScope(positional, *dbFlag, *bucketFlag)
	if err != nil {
		return err
	}

	summary, err := runDumpBuckets(h, database, bucket, *out, h.terminal, nil)
	if err != nil {
		return err
	}
	printDumpSummary(h.terminal, summary)
	_, _ = fmt.Fprintln(h.terminal, legacyReplDumpNote)
	if code := summary.exitCode(); code != ExitOK {
		return fmt.Errorf("dump finished with %d failed bucket(s); see the failures above (exit code %d outside the REPL)",
			len(summary.failures), code)
	}
	return nil
}

// printDumpSummary writes the human-readable progress/result line.
func printDumpSummary(w io.Writer, summary *dumpSummary) {
	w = inertWriter{w}
	_, _ = fmt.Fprintf(w, "dump: %d database(s), %d bucket(s), %d document(s), %d byte(s)",
		len(summary.databases), summary.buckets, summary.documents, summary.bytes)
	if summary.incomplete {
		_, _ = fmt.Fprintf(w, " (incomplete: %d bucket(s) failed)", len(summary.failures))
	}
	_, _ = fmt.Fprintln(w)
	for _, note := range summary.warnings {
		_, _ = fmt.Fprintf(w, "  warning: %s\n", note)
	}
	for _, f := range summary.failures {
		_, _ = fmt.Fprintf(w, "  FAILED %s/%s: %s\n", f.Database, f.Bucket, f.Error)
	}
}

// runDumpBuckets resolves the requested scope to a bucket list, dumps each
// one in turn (sequential by design — dump-restore-spec.md §15: "Parallel
// per-bucket dumps" is a non-goal), and writes manifest.json. A bucket that
// cannot be dumped is recorded and skipped rather than aborting the whole
// run (§16.4); interrupted is checked between chunks and between buckets so
// Ctrl-C leaves a manifest marked incomplete instead of a half-written file
// with no record of what happened (§6: "Ctrl-C is safe").
func runDumpBuckets(h *CommandHandler, database, bucket, outDir string, progress io.Writer, interrupted <-chan struct{}) (*dumpSummary, error) {
	progress = inertWriter{progress}
	if err := checkOutDir(outDir); err != nil {
		return nil, &UsageError{Message: err.Error()}
	}

	targets, err := resolveDumpTargets(h, database, bucket)
	if err != nil {
		return nil, fmt.Errorf("resolving dump scope: %w", err)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("nothing to dump: %s has no buckets", scopeLabel(database, bucket))
	}

	summary := newDumpSummary()
	warnedSourceNode := false

	for _, target := range targets {
		if interrupted != nil {
			select {
			case <-interrupted:
				summary.interrupted = true
			default:
			}
		}
		if summary.interrupted {
			summary.incomplete = true
			summary.failures = append(summary.failures, dumpFailure{
				Database: target.Database, Bucket: target.Bucket, Error: "interrupted before this bucket started",
			})
			continue
		}

		outcome := dumpOneBucket(h, target, outDir, progress, interrupted)
		if outcome.err != nil {
			if errors.Is(outcome.err, errDumpInterrupted) {
				summary.interrupted = true
			}
			summary.incomplete = true
			summary.failures = append(summary.failures, dumpFailure{
				Database: target.Database, Bucket: target.Bucket, Error: outcome.err.Error(),
			})
			continue
		}

		if summary.sourceNode == "" {
			summary.sourceNode = outcome.sourceNode
		}
		if !warnedSourceNode {
			warnedSourceNode = true
			warnIfDifferentSourceNode(outDir, outcome.sourceNode, progress)
		}
		summary.addDatabase(target.Database)
		summary.buckets++
		summary.documents += outcome.documents
		summary.bytes += outcome.bytes
	}

	// Warn from what was actually written, so the set named here is exactly
	// the set restore will name from the same directory.
	blobBuckets, scanErr := blobBucketsMissingPayloads(outDir)
	if scanErr != nil {
		_, _ = fmt.Fprintf(progress, "warning: could not fully check this dump for blob buckets: %v\n", scanErr)
	}
	if note := blobPayloadNote(blobConsequenceDump, blobBuckets); note != "" {
		summary.warnings = append(summary.warnings, note)
		_, _ = fmt.Fprintln(progress, "warning:", note)
	}

	manifest := topLevelManifest{
		FormatVersion:       dumpManifestFormatVersionCLI,
		HoardDBVersion:      server.Version,
		CreatedAt:           time.Now().UTC().Format(time.RFC3339),
		SourceNode:          summary.sourceNode,
		Databases:           summary.databases,
		Buckets:             summary.buckets,
		Documents:           summary.documents,
		Bytes:               summary.bytes,
		BlobsIncluded:       false,
		CredentialsIncluded: false,
		GloballyConsistent:  false,
		Incomplete:          summary.incomplete,
		Failures:            summary.failures,
	}
	if err := writeManifest(outDir, manifest); err != nil {
		return summary, fmt.Errorf("writing manifest.json: %w", err)
	}
	return summary, nil
}

// dumpManifestFormatVersionCLI mirrors server.dumpManifestFormatVersion; the
// CLI cannot import the server's unexported constant, and both sides bump
// together since the layout is one artifact (dump-restore-spec.md §4). It is
// the format a dump has: manifest.json is stamped with it, and every
// bucket's metadata.json with the server's copy. Format 3 added the
// <bucket>.dead.bson file (claim-ack-spec §7.2).
const dumpManifestFormatVersionCLI = 3

// restorableFormatCLI mirrors the server's restorableFormat, and is kept in
// step with it: 2, which developers hold, and 3. There is no format 1 (owner
// ruling 2026-09-30), and restoring one would declare a unique index without
// its uniqueness — see the server's constant. An older CLI that accepts only 2
// refuses a format-3 dump here, which is the point of the bump.
func restorableFormatCLI(version int) bool {
	return version == 2 || version == dumpManifestFormatVersionCLI
}

// The two consequence clauses blobPayloadNote renders. Blob payloads are in
// no dump today (dumpOneBucket writes whatever OpDumpChunk returns, and the
// server reports BlobsIncluded false for every bucket — server/dump.go), so
// both sides of a backup have to say so out loud. Enumerating and capturing
// the payloads is the recorded follow-on; warning is what stops an operator
// discovering it during a restore.
const (
	// blobFieldLabel marks a blobStore field in the scan's entries.
	blobFieldLabel = "blobStore field"

	blobConsequenceDump    = "were written to this dump as keys and metadata only"
	blobConsequenceRestore = "in this dump have no payload bytes to restore"
)

// blobPayloadNote builds the per-run warning for blob buckets whose payload
// bytes are not in the dump. Each bucket is named: an operator told that
// "some buckets" lost their payloads has to go and work out which, and the
// scan already knows.
//
// Empty when there is nothing to warn about, so a dump with no blob bucket
// stays quiet rather than teaching operators to skim past a standing notice.
func blobPayloadNote(consequence string, blobBuckets []string) string {
	if len(blobBuckets) == 0 {
		return ""
	}
	fieldClause := ""
	for _, bucket := range blobBuckets {
		if strings.Contains(bucket, blobFieldLabel) {
			fieldClause = " A blobStore field's elements are not restored at all, so the restored field is empty."
			break
		}
	}
	return fmt.Sprintf(
		"blob payloads are not captured by any hoardDB dump yet: %d blob bucket(s) %s — %s. "+
			"A restore recreates those buckets empty, with no keys, no metadata and no payload bytes; "+
			"the keys and metadata remain in the dump directory's .bson and .metadata.json files.%s "+
			"Copy the blob data out of band before relying on this backup.",
		len(blobBuckets), consequence, strings.Join(blobBuckets, ", "), fieldClause)
}

// blobBucketsMissingPayloads reports every "<database>/<bucket>" in a dump
// directory that is a blob bucket whose payloads were not captured, sorted,
// and every bucket that declares a blobStore field, whose elements restore
// skips. The second kind is named with its fields:
// "app/Post (blobStore field Photos)".
//
// The answer is read from the dump's own metadata files rather than from a
// server round trip, so dump and restore warn about exactly the same set —
// the artifact is the shared truth, and restore has no server-side store type
// to ask for. It is also self-retiring: the gate is `blobs_included == false`,
// so when a later change actually captures the payloads the warning stops on
// its own instead of becoming a lie.
//
// A metadata file that cannot be read or parsed is reported in the returned
// error and skipped, not fatal: this is a warning scanner, and refusing the
// whole warning over one unreadable file would hide the buckets it did find.
// Restore reports the same file again, with full context, when it reaches it
// (restoreOneBucket).
func blobBucketsMissingPayloads(dir string) ([]string, error) {
	dbEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading dump directory %q: %w", dir, err)
	}

	var blobBuckets []string
	var scanErrs []error
	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() {
			continue
		}
		database := dbEntry.Name()
		dbDir := filepath.Join(dir, database)
		bucketEntries, err := os.ReadDir(dbDir)
		if err != nil {
			scanErrs = append(scanErrs, fmt.Errorf("reading database directory %q: %w", database, err))
			continue
		}
		for _, be := range bucketEntries {
			name := be.Name()
			if be.IsDir() || !strings.HasSuffix(name, ".metadata.json") {
				continue
			}
			metaPath := filepath.Join(dbDir, name)
			metaRaw, err := os.ReadFile(metaPath)
			if err != nil {
				scanErrs = append(scanErrs, fmt.Errorf("reading %s: %w", metaPath, err))
				continue
			}
			var meta bucketMetadataFile
			if err := json.Unmarshal(metaRaw, &meta); err != nil {
				scanErrs = append(scanErrs, fmt.Errorf("parsing %s: %w", metaPath, err))
				continue
			}
			if meta.BlobsIncluded {
				continue
			}
			bucket := database + "/" + strings.TrimSuffix(name, ".metadata.json")
			switch fields := server.BlobFieldNames(meta.Stores); {
			case meta.StoreType == string(server.BucketTypeBlob):
				blobBuckets = append(blobBuckets, bucket)
			case len(fields) > 0:
				blobBuckets = append(blobBuckets, fmt.Sprintf("%s (%s %s)", bucket, blobFieldLabel, strings.Join(fields, ", ")))
			}
		}
	}
	sort.Strings(blobBuckets)
	return blobBuckets, errors.Join(scanErrs...)
}

func scopeLabel(database, bucket string) string {
	switch {
	case bucket != "":
		return fmt.Sprintf("%s/%s", database, bucket)
	case database != "":
		return database
	default:
		return "the server"
	}
}

// checkOutDir refuses a non-empty --out that is not itself a previous dump
// (dump-restore-spec.md §6, §13): silently co-mingling two dumps in one
// directory produces something that restores as neither.
func checkOutDir(outDir string) error {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(outDir, privateDumpDirMode)
		}
		return fmt.Errorf("reading --out directory %q: %w", outDir, err)
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); err != nil {
		return fmt.Errorf("--out %q is not empty and is not a previous dump (no manifest.json found); pass an empty directory, or point --out at an existing dump to add to it", outDir)
	}
	return nil
}

// warnIfDifferentSourceNode prints (never refuses) when --out already holds
// a dump from a different source_node (dump-restore-spec.md, task-2 brief's
// user decision).
func warnIfDifferentSourceNode(outDir, currentSourceNode string, progress io.Writer) {
	progress = inertWriter{progress}
	data, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		return
	}
	var previous topLevelManifest
	if err := json.Unmarshal(data, &previous); err != nil {
		return
	}
	if previous.SourceNode != "" && currentSourceNode != "" && previous.SourceNode != currentSourceNode {
		_, _ = fmt.Fprintf(progress, "warning: %s already holds a dump from source_node %q; this dump is from %q — proceeding\n",
			outDir, previous.SourceNode, currentSourceNode)
	}
}

// resolveDumpTargets figures out which (database, bucket) pairs to dump.
//
// Whole-cluster scope: OpListDBs/OpListBuckets answer for the whole cluster
// (BUG-0468), and dump forwards each bucket's start to its owner, so a scope
// wider than a single bucket covers every bucket whether or not the connected
// node holds it. If a member is down the listing is refused and the refusal
// names it, which is the CLI's report; there is no silent subset to warn about.
func resolveDumpTargets(h *CommandHandler, database, bucket string) ([]dumpBucketTarget, error) {
	if bucket != "" {
		return []dumpBucketTarget{{Database: database, Bucket: bucket}}, nil
	}

	var databases []string
	if database != "" {
		databases = []string{database}
	} else {
		var resp server.ListDatabasesResponse
		if err := h.send(transport.OpListDBs, &server.ListDatabasesRequest{}, &resp); err != nil {
			return nil, fmt.Errorf("listing databases: %w", err)
		}
		for _, db := range resp.Databases {
			databases = append(databases, db.Name)
		}
	}

	var targets []dumpBucketTarget
	for _, db := range databases {
		var resp server.ListBucketsResponse
		if err := h.send(transport.OpListBuckets, &server.ListBucketsRequest{Database: db}, &resp); err != nil {
			return nil, fmt.Errorf("listing buckets in %q: %w", db, err)
		}
		for _, b := range resp.Buckets {
			targets = append(targets, dumpBucketTarget{Database: db, Bucket: b.Name})
		}
	}
	return targets, nil
}

// bucketDumpOutcome is one bucket's result.
type bucketDumpOutcome struct {
	sourceNode string
	documents  int64
	bytes      int64
	err        error
}

// dumpOneBucket drives OpDumpStart/Chunk/Done for one bucket: writes
// <database>/<bucket>.bson as the concatenation of whatever OpDumpChunk
// returns (already whole documents, never split — server/dump.go), computes
// the SHA-256 digest over exactly those bytes, and writes
// <database>/<bucket>.metadata.json once the server acknowledges it.
func dumpOneBucket(h *CommandHandler, target dumpBucketTarget, outDir string, progress io.Writer, interrupted <-chan struct{}) bucketDumpOutcome {
	progress = inertWriter{progress}
	var startResp server.DumpStartResponse
	if err := h.send(transport.OpDumpStart, &server.DumpStartRequest{Database: target.Database, Bucket: target.Bucket, AcceptStores: true, AcceptDead: true}, &startResp); err != nil {
		return bucketDumpOutcome{err: fmt.Errorf("dump start: %w", err)}
	}
	if startResp.Manifest == nil {
		return bucketDumpOutcome{err: fmt.Errorf("dump start: server returned no manifest")}
	}
	manifest := *startResp.Manifest

	dbDir := filepath.Join(outDir, target.Database)
	if err := os.MkdirAll(dbDir, privateDumpDirMode); err != nil {
		abortDump(h, target)
		return bucketDumpOutcome{err: fmt.Errorf("creating %s: %w", dbDir, err)}
	}
	bsonPath := filepath.Join(dbDir, target.Bucket+".bson")
	f, err := createPrivateFile(bsonPath)
	if err != nil {
		abortDump(h, target)
		return bucketDumpOutcome{err: fmt.Errorf("creating %s: %w", bsonPath, err)}
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(progress, "dumping %s/%s...\n", target.Database, target.Bucket)
	// A successful start's message is a note about what the dump leaves out:
	// an index on a path, which this release cannot declare again.
	if startResp.Message != "" {
		_, _ = fmt.Fprintf(progress, "note: %s\n", startResp.Message)
	}

	hasher := sha256.New()
	var totalBytes int64
	// A format-2 bucket's element records go to their own file, which is
	// created only when the first of them arrives.
	stores := sectionFileWriter{path: filepath.Join(dbDir, target.Bucket+".stores.bson"), hasher: sha256.New()}
	defer func() { _ = stores.close() }()
	// Likewise a format-3 bucket's dead entries, in a file of their own: never
	// in the documents file, where a restore would push them back alive.
	dead := sectionFileWriter{path: filepath.Join(dbDir, target.Bucket+".dead.bson"), hasher: sha256.New()}
	defer func() { _ = dead.close() }()
	for {
		if interrupted != nil {
			select {
			case <-interrupted:
				abortDump(h, target)
				return bucketDumpOutcome{err: errDumpInterrupted}
			default:
			}
		}

		var chunk server.DumpChunkResponse
		if err := h.send(transport.OpDumpChunk, &server.DumpChunkRequest{Database: target.Database, Bucket: target.Bucket}, &chunk); err != nil {
			abortDump(h, target)
			return bucketDumpOutcome{err: fmt.Errorf("dump chunk: %w", err)}
		}
		if len(chunk.Data) > 0 {
			switch chunk.Section {
			case "":
				if _, err := f.Write(chunk.Data); err != nil {
					abortDump(h, target)
					return bucketDumpOutcome{err: fmt.Errorf("writing %s: %w", bsonPath, err)}
				}
				hasher.Write(chunk.Data)
				totalBytes += int64(len(chunk.Data))
			case "stores":
				if err := stores.write(chunk.Data); err != nil {
					abortDump(h, target)
					return bucketDumpOutcome{err: err}
				}
			case "dead":
				if err := dead.write(chunk.Data); err != nil {
					abortDump(h, target)
					return bucketDumpOutcome{err: err}
				}
			default:
				// A section this CLI does not write would otherwise be appended to
				// the documents file, and a restore would push it back as documents.
				abortDump(h, target)
				return bucketDumpOutcome{err: fmt.Errorf("dump chunk: the server sent a %q section, which this hoarddb does not know how to write. Upgrade hoarddb to this server's version and dump again", chunk.Section)}
			}
		}
		if chunk.EOF {
			break
		}
	}
	// Close before acknowledging to the server, and check the error: the
	// bytes the filesystem kept are what a restore reads, while the digest
	// below covers the bytes the server sent. A flush error swallowed here
	// would leave a short .bson whose manifest digest still matches, so a
	// failed close aborts this bucket like any other transfer failure.
	// (The deferred close is the safety net for the early-return error
	// paths above; a second close returns an error the repo ignores by
	// convention.)
	if err := f.Close(); err != nil {
		abortDump(h, target)
		return bucketDumpOutcome{err: fmt.Errorf("closing %s: %w", bsonPath, err)}
	}

	digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if err := stores.close(); err != nil {
		abortDump(h, target)
		return bucketDumpOutcome{err: err}
	}
	storesDigest := stores.digest()
	if err := dead.close(); err != nil {
		abortDump(h, target)
		return bucketDumpOutcome{err: err}
	}
	deadDigest := dead.digest()

	var doneResp server.DumpDoneResponse
	if err := h.send(transport.OpDumpDone, &server.DumpDoneRequest{Database: target.Database, Bucket: target.Bucket, Digest: digest, StoresDigest: storesDigest, DeadDigest: deadDigest}, &doneResp); err != nil {
		return bucketDumpOutcome{err: fmt.Errorf("dump done: %w", err)}
	}

	// Documents comes from OpDumpDone's response — the tally the server's
	// one real walk actually emitted — never from OpDumpStart's manifest,
	// which reports no count at all (fix-round-1: an upfront DocCount()
	// was a second, independent Badger transaction that could silently
	// disagree with what this walk streamed; see server/dump.go's
	// DumpBucketManifest doc comment).
	meta := bucketMetadataFile{
		FormatVersion:  manifest.FormatVersion,
		HoardDBVersion: manifest.HoardDBVersion,
		ProtocolMajor:  manifest.ProtocolMajor,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		SourceNode:     manifest.SourceNode,
		Database:       manifest.Database,
		Bucket:         manifest.Bucket,
		StoreType:      string(manifest.StoreType),
		Indexes:        manifest.Indexes,
		IndexSpecs:     manifest.IndexSpecs,
		Documents:      doneResp.Documents,
		Bytes:          totalBytes,
		Digest:         digest,
		BlobsIncluded:  manifest.BlobsIncluded,
		Stores:         manifest.Stores,
		Elements:       doneResp.Elements,
		StoresBytes:    stores.bytes,
		StoresDigest:   storesDigest,
		Dead:           doneResp.Dead,
		DeadBytes:      dead.bytes,
		DeadDigest:     deadDigest,

		ReplicationFactor: manifest.ReplicationFactor,
		Placement:         manifest.Placement,
	}
	metaPath := filepath.Join(dbDir, target.Bucket+".metadata.json")
	if err := writeJSONFile(metaPath, meta); err != nil {
		return bucketDumpOutcome{err: fmt.Errorf("writing %s: %w", metaPath, err)}
	}

	_, _ = fmt.Fprintf(progress, "  %s/%s: %d documents, %d bytes, %s\n", target.Database, target.Bucket, meta.Documents, totalBytes, digest)
	if meta.Dead > 0 {
		_, _ = fmt.Fprintf(progress, "  %s/%s: %d dead entries, kept dead in %s.dead.bson\n", target.Database, target.Bucket, meta.Dead, target.Bucket)
	}
	return bucketDumpOutcome{sourceNode: manifest.SourceNode, documents: meta.Documents, bytes: totalBytes}
}

// sectionFileWriter writes one of a bucket's dump sections ("stores" or
// "dead") to its own file, created on the first chunk, and digests exactly the
// bytes it wrote.
type sectionFileWriter struct {
	path   string
	file   *os.File
	hasher hash.Hash
	bytes  int64
	closed bool
}

func (w *sectionFileWriter) write(data []byte) error {
	if w.file == nil {
		file, err := createPrivateFile(w.path)
		if err != nil {
			return fmt.Errorf("creating %s: %w", w.path, err)
		}
		w.file = file
	}
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("writing %s: %w", w.path, err)
	}
	w.hasher.Write(data)
	w.bytes += int64(len(data))
	return nil
}

// close closes the file once, reporting the close's error: a flush error
// swallowed here would leave a short file whose recorded digest matches.
func (w *sectionFileWriter) close() error {
	if w.file == nil || w.closed {
		return nil
	}
	w.closed = true
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", w.path, err)
	}
	return nil
}

// digest is the section's "sha256:<hex>", or "" when it had no element.
func (w *sectionFileWriter) digest() string {
	if w.bytes == 0 {
		return ""
	}
	return "sha256:" + hex.EncodeToString(w.hasher.Sum(nil))
}

// abortDump tells the server to release the bucket's session even though
// this bucket's dump failed client-side, so a failed dump does not block the
// next one (server/dump.go's OpDumpDone releases unconditionally, and an
// empty Digest skips the cross-check since there is nothing meaningful to
// compare). Best effort: if this also fails, the server's own pin-ceiling
// and connection-close watchers are the backstop.
func abortDump(h *CommandHandler, target dumpBucketTarget) {
	_ = h.send(transport.OpDumpDone, &server.DumpDoneRequest{Database: target.Database, Bucket: target.Bucket}, nil)
}

// abortRestore is abortDump's restore-side twin: it tells the server to
// release the bucket's restore session and pin even though this bucket's
// restore failed client-side (an interrupted transfer, a chunk send
// failure), so a failed attempt does not block an immediate retry.
//
// This matters more for restore than it does for dump: the standalone CLI
// process exits right after a failure either way, which closes the
// connection and lets the server's own connection-close watcher
// (watchRestoreConnection) release the pin — but the REPL keeps the
// connection open across statements, so without this call the user's very
// next `restore` on the same bucket would be refused with
// ErrRestoreBucketConflict for a reason they cannot see (AGENTS.md rule 8,
// "interactive by default where it helps": a tool that makes you close and
// reopen the session to retry is not pleasant to live in). Best effort: if
// this also fails, the server's own connection-close watcher is the
// backstop, same as abortDump.
func abortRestore(h *CommandHandler, target restoreBucketTarget, sessionID string) {
	if sessionID == "" {
		return
	}
	_ = h.send(transport.OpRestoreDone, &server.RestoreDoneRequest{
		Database: target.Database, Bucket: target.Bucket, SessionID: sessionID,
	}, nil)
}

// writeManifest writes manifest.json to outDir.
func writeManifest(outDir string, manifest topLevelManifest) error {
	return writeJSONFile(filepath.Join(outDir, "manifest.json"), manifest)
}

// writeJSONFile marshals v as indented JSON and writes it to path.
func writeJSONFile(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	data = append(data, '\n')
	file, err := createPrivateFile(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}
	return nil
}

// The `restore` verb (dump-restore-spec.md §9). Restore consumes untrusted
// input — a dump directory can be copied to a laptop and back, or be
// deliberately hostile — so every check below refuses (and, on a
// per-bucket basis, continues past the refused bucket rather than aborting
// the whole run, mirroring `dump`'s own §16.4 "continue past a bad bucket,
// mark incomplete" philosophy) before that bucket's data is ever sent to
// the server. The server (server/restore.go) re-runs the authoritative half
// of these same checks — format version, path safety, the reserved
// namespace, store type, disk space — so a hand-rolled client that skips
// this CLI cannot skip the validation.

// restoreBucketTarget names one bucket found in a dump directory to restore,
// discovered by walking the directory rather than trusting a manifest
// field: dump-restore-spec.md §9.1 treats every string inside the artifact
// as untrusted, so a target's Database/Bucket here come from the actual
// `<dir>/<database>/<bucket>.metadata.json` path that was found, and are
// validated again (validateRestoreReadPath) before being joined into any
// further path.
type restoreBucketTarget struct {
	Database string
	Bucket   string
}

// restoreCLIOptions carries the restore verb's flags into the shared driver
// (runRestoreBuckets), used by both the standalone verb and the REPL
// statement.
type restoreCLIOptions struct {
	Dir             string
	Drop            bool
	AllowIncomplete bool
	DryRun          bool
}

// restoreResult is the single JSON object --json prints to stdout for
// restore, the same shape's spirit as dumpResult (dump-restore-spec.md §6):
// one object on stdout, progress on stderr. UsersRecreated and
// UsersAwaitingPassword are always zero — account restore is Task 4's
// territory (see the users.json warning below) — reported anyway so a
// script parsing this JSON does not need a schema that differs from
// dump's.
type restoreResult struct {
	OK                    bool          `json:"ok"`
	Databases             []string      `json:"databases"`
	Buckets               int           `json:"buckets"`
	DocumentsApplied      int64         `json:"documents_applied"`
	DocumentsSkipped      int64         `json:"documents_skipped"`
	ElementsSkipped       int64         `json:"elements_skipped"`
	UsersRecreated        int           `json:"users_recreated"`
	UsersAwaitingPassword int           `json:"users_awaiting_password"`
	DryRun                bool          `json:"dry_run,omitempty"`
	Warnings              []string      `json:"warnings,omitempty"`
	Errors                []dumpFailure `json:"errors"`
	// FactorNotRestored, FactorKept and FactorLowered name, as
	// database/bucket, every bucket that did not end up with the replication
	// factor its dump records; each is absent when it would be empty.
	FactorNotRestored []string `json:"factor_not_restored,omitempty"`
	FactorKept        []string `json:"factor_kept,omitempty"`
	FactorLowered     []string `json:"factor_lowered,omitempty"`
}

// restoreSummary accumulates the outcome of every bucket a restore run
// attempted.
type restoreSummary struct {
	seenDBs          map[string]bool
	databases        []string
	buckets          int
	documentsApplied int64
	documentsSkipped int64
	// elementsSkipped counts store-field elements restore received and did
	// not write: today exactly a blobStore field's, whose payload no dump has.
	elementsSkipped int64
	failures        []dumpFailure
	// restored names each bucket restored, as database/bucket, so a run that
	// stopped part way — a later bucket's start refused while a node joins the
	// cluster — says which buckets landed (join-freeze spec §7.3).
	restored []string
	// warnings must appear in both the text summary and --json (never only
	// a log line) — the brief is explicit that a silently-ignored identity
	// file is exactly the failure mode dump-restore-spec.md §5 exists to
	// prevent, so the users.json notice below is carried here rather than
	// only printed to stderr.
	warnings    []string
	incomplete  bool
	interrupted bool
	dryRun      bool
	// factorNotRestored, factorKept and factorLowered are the buckets whose
	// replication factor is not the one their dump records: printed once, at
	// the end, because a line per bucket in a long restore scrolls away.
	factorNotRestored []factorNote
	factorKept        []factorNote
	factorLowered     []factorNote
}

// factorNote is one bucket whose factor a restore did not carry over: the
// factor the dump records and the one the bucket kept or had.
type factorNote struct {
	database, bucket string
	dumped, bucketed int
}

// factorNotesNamed is how many buckets a factor block names before "and N more".
const factorNotesNamed = 10

// qualifiedNames is each note's database/bucket, the --json form.
func qualifiedNames(notes []factorNote) []string {
	names := make([]string, 0, len(notes))
	for _, note := range notes {
		names = append(names, note.database+"/"+note.bucket)
	}
	return names
}

// printFactorBlock writes "<verb>: N bucket(s) <heading>", then at most
// factorNotesNamed notes as describe renders each, then advice.
func printFactorBlock(w io.Writer, verb, heading string, notes []factorNote, describe func(factorNote) string, advice string) {
	w = inertWriter{w}
	if len(notes) == 0 {
		return
	}
	_, _ = fmt.Fprintf(w, "%s: %d bucket(s) %s\n", verb, len(notes), heading)
	named := make([]string, 0, factorNotesNamed)
	for _, note := range notes[:min(len(notes), factorNotesNamed)] {
		named = append(named, describe(note))
	}
	line := strings.Join(named, ", ")
	if len(notes) > factorNotesNamed {
		line += fmt.Sprintf(", and %d more", len(notes)-factorNotesNamed)
	}
	_, _ = fmt.Fprintf(w, "  %s\n  %s\n", line, advice)
}

// factorWording is the tense of the factor blocks: what a restore did, or,
// under --dry-run, which creates and drops nothing, what it would do.
type factorWording struct {
	verb, notRestored, kept, keptAt, lowered, loweredFrom string
}

var (
	factorWordingDone = factorWording{
		verb:        "restore",
		notRestored: "were restored WITHOUT the replication factor their dump records",
		kept:        "kept their own replication factor; the dump records another",
		keptAt:      "kept",
		lowered:     "had their replication factor LOWERED by --drop",
		loweredFrom: "was",
	}
	factorWordingDryRun = factorWording{
		verb:        "restore --dry-run",
		notRestored: "would be restored without the replication factor their dump records",
		kept:        "would keep their own replication factor; the dump records another",
		keptAt:      "would keep",
		lowered:     "would have their replication factor LOWERED by --drop",
		loweredFrom: "is",
	}
)

// printFactorBlocks ends a restore's summary with the buckets whose factor is
// not the one their dump records.
func printFactorBlocks(w io.Writer, summary *restoreSummary) {
	w = inertWriter{w}
	wording := factorWordingDone
	if summary.dryRun {
		wording = factorWordingDryRun
	}
	printFactorBlock(w, wording.verb, wording.notRestored, summary.factorNotRestored,
		func(note factorNote) string {
			return fmt.Sprintf("%s.%s (%d)", note.database, note.bucket, note.dumped)
		},
		"This server cannot place that many copies (it is standalone, or has fewer members than the factor), so they use the node default. "+
			"Add members ('join' on each new node, then 'nodes' to count them), then run this restore again with --drop.")
	printFactorBlock(w, wording.verb, wording.kept, summary.factorKept,
		func(note factorNote) string {
			return fmt.Sprintf("%s.%s (%s %d; the dump records %d)", note.database, note.bucket, wording.keptAt, note.bucketed, note.dumped)
		},
		"A restore into an existing bucket restores documents, not placement. To use the dump's factor, run this restore again with --drop.")
	printFactorBlock(w, wording.verb, wording.lowered, summary.factorLowered,
		func(note factorNote) string {
			return fmt.Sprintf("%s.%s (%s %d; the dump records none)", note.database, note.bucket, wording.loweredFrom, note.bucketed)
		},
		"The dump records no factor for them. To restore one, drop the bucket, create it with { replication_factor: N }, "+
			"and run this restore again without --drop.")
}

func newRestoreSummary() *restoreSummary { return &restoreSummary{seenDBs: make(map[string]bool)} }

func (sm *restoreSummary) addDatabase(name string) {
	if !sm.seenDBs[name] {
		sm.seenDBs[name] = true
		sm.databases = append(sm.databases, name)
	}
}

// exitCode mirrors dumpSummary.exitCode's mapping (dump-restore-spec.md §6):
// 0 clean, 3 partial/verification, 2 nothing succeeded, 4 interrupted.
func (sm *restoreSummary) exitCode() int {
	switch {
	case sm.interrupted:
		return ExitInterrupted
	case len(sm.failures) == 0 && sm.documentsSkipped == 0 && sm.elementsSkipped == 0:
		return ExitOK
	case len(sm.failures) == 0:
		// Skipped data was left behind, not restored: a clean exit would let
		// a script treat an incomplete restore as a good one.
		return ExitPartial
	case sm.buckets == 0:
		return ExitRefused
	default:
		return ExitPartial
	}
}

// RunRestoreCommand implements the standalone `hoarddb restore` verb: parses
// its own connection flags (the same ones RunDumpCommand accepts) plus the
// restore-specific flags, connects, runs the restore, prints either a text
// summary or (with --json) exactly one JSON object to stdout, and returns
// the process exit code.
func RunRestoreCommand(args []string) int {
	installClientLogger(false)
	fs := flag.NewFlagSet("hoarddb restore", flag.ContinueOnError)
	var usage strings.Builder
	fs.SetOutput(&usage)

	connection := registerConnectionFlags(fs)
	dir := fs.String("dir", "./dump", "source directory")
	drop := fs.Bool("drop", false, "drop each bucket before restoring it")
	replaceUsers := fs.Bool("replace-users", false, "replace an existing account's enabled state and roles from the dump (not yet implemented — no-op; accepted for forward compatibility)")
	allowIncomplete := fs.Bool("allow-incomplete", false, "restore from a manifest marked incomplete")
	users := fs.Bool("users", true, "recreate users.json if present (not yet implemented — no-op; accepted for forward compatibility)")
	noUsers := fs.Bool("no-users", false, "skip users.json (not yet implemented — no-op; accepted for forward compatibility)")
	withCredentials := fs.Bool("with-credentials", false, "restore password material for a same-installation restore (not yet implemented — no-op; accepted for forward compatibility)")
	jsonOut := fs.Bool("json", false, "print one JSON result object to stdout instead of a text summary")
	dryRun := fs.Bool("dry-run", false, "validate every file and report what would change; write nothing")

	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "usage: hoarddb restore [flags]\n%s\n", usage.String())
		return ExitUsage
	}
	if len(positional) > 0 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: hoarddb restore takes no positional arguments; name the source with --dir")
		return ExitUsage
	}
	_, _, _, _ = replaceUsers, users, noUsers, withCredentials

	// Progress and any incidental connection chatter go to stderr, never
	// stdout (dump-restore-spec.md §6): --json's caller pipes stdout to a
	// parser.
	spec := connection.spec(flagWasProvided(fs, "address"))
	spec.Chatter = os.Stderr
	c, err := connectSession(spec)
	if err != nil {
		return verbConnectFailure(os.Stderr, c, err)
	}
	defer c.Close()
	c.notef("Connected to %s\n", c.addr)

	interrupted := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	go func() {
		if _, ok := <-sig; ok {
			close(interrupted)
		}
	}()

	restoreOpts := restoreCLIOptions{Dir: *dir, Drop: *drop, AllowIncomplete: *allowIncomplete, DryRun: *dryRun}
	summary, runErr := runRestoreBuckets(c.handler, restoreOpts, os.Stderr, interrupted)
	if runErr != nil {
		// Every error runRestoreBuckets returns happened before a bucket was
		// touched: an unreadable dump directory, a manifest it cannot use. That
		// is unreadable input (1); 3 is reserved for "something was applied".
		_, _ = fmt.Fprintf(os.Stderr, "restore: %s\n", terminalSafeText(runErr.Error()))
		return ExitUsage
	}

	code := summary.exitCode()
	if *jsonOut {
		payload, _ := json.Marshal(summary.result(code))
		_, _ = fmt.Fprintln(os.Stdout, string(payload))
	} else {
		printRestoreSummary(os.Stderr, summary)
	}
	return code
}

// handleRestore is the REPL entry point: `restore [flags]` as a statement
// inside an interactive session, sharing runRestoreBuckets with the
// standalone verb.
func (h *CommandHandler) handleRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", "./dump", "source directory")
	drop := fs.Bool("drop", false, "drop each bucket before restoring it")
	fs.Bool("replace-users", false, "not yet implemented")
	allowIncomplete := fs.Bool("allow-incomplete", false, "restore from a manifest marked incomplete")
	fs.Bool("users", true, "not yet implemented")
	fs.Bool("no-users", false, "not yet implemented")
	fs.Bool("with-credentials", false, "not yet implemented")
	fs.Bool("json", false, "not used in the REPL form")
	dryRun := fs.Bool("dry-run", false, "validate every file and report what would change; write nothing")
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return fmt.Errorf("usage: restore [--dir <dir>] [--drop] [--allow-incomplete] [--dry-run]: %w", err)
	}
	if len(positional) > 0 {
		return fmt.Errorf("usage: restore takes no positional arguments; name the source with --dir")
	}

	summary, err := runRestoreBuckets(h, restoreCLIOptions{Dir: *dir, Drop: *drop, AllowIncomplete: *allowIncomplete, DryRun: *dryRun}, h.terminal, nil)
	if err != nil {
		return err
	}
	printRestoreSummary(h.terminal, summary)
	if code := summary.exitCode(); code != ExitOK {
		return fmt.Errorf("restore finished with %d failed bucket(s), %d skipped document(s) and %d skipped store-field element(s); see above (exit code %d outside the REPL)",
			len(summary.failures), summary.documentsSkipped, summary.elementsSkipped, code)
	}
	return nil
}

// result is the --json object for a finished run whose exit code is code.
func (sm *restoreSummary) result(code int) restoreResult {
	return restoreResult{
		OK:               code == ExitOK,
		Databases:        nonNil(sm.databases),
		Buckets:          sm.buckets,
		DocumentsApplied: sm.documentsApplied,
		DocumentsSkipped: sm.documentsSkipped,
		ElementsSkipped:  sm.elementsSkipped,
		DryRun:           sm.dryRun,
		Warnings:         sm.warnings,
		Errors:           nonNil(sm.failures),

		FactorNotRestored: qualifiedNames(sm.factorNotRestored),
		FactorKept:        qualifiedNames(sm.factorKept),
		FactorLowered:     qualifiedNames(sm.factorLowered),
	}
}

// printRestoreSummary writes the human-readable progress/result line.
func printRestoreSummary(w io.Writer, summary *restoreSummary) {
	w = inertWriter{w}
	verb := "restore"
	if summary.dryRun {
		verb = "restore --dry-run"
	}
	_, _ = fmt.Fprintf(w, "%s: %d database(s), %d bucket(s), %d document(s) applied, %d skipped",
		verb, len(summary.databases), summary.buckets, summary.documentsApplied, summary.documentsSkipped)
	if summary.elementsSkipped > 0 {
		_, _ = fmt.Fprintf(w, ", %d store-field element(s) skipped", summary.elementsSkipped)
	}
	if summary.incomplete {
		_, _ = fmt.Fprintf(w, " (incomplete: %d bucket(s) failed)", len(summary.failures))
	}
	_, _ = fmt.Fprintln(w)
	for _, note := range summary.warnings {
		_, _ = fmt.Fprintf(w, "  warning: %s\n", note)
	}
	if summary.incomplete && !summary.dryRun {
		for _, bucket := range summary.restored {
			_, _ = fmt.Fprintf(w, "  restored %s\n", bucket)
		}
	}
	for _, f := range summary.failures {
		_, _ = fmt.Fprintf(w, "  FAILED %s/%s: %s\n", f.Database, f.Bucket, f.Error)
	}
	printFactorBlocks(w, summary)
}

// runRestoreBuckets reads manifest.json, enumerates every
// <database>/<bucket>.metadata.json under opts.Dir, and restores each in
// turn (sequential by design, matching dump — dump-restore-spec.md §15).
// A bucket that fails validation or is refused by the server is recorded
// and the run continues to the next bucket rather than aborting outright,
// the same "continue past a bad bucket, mark incomplete" choice dump-side
// makes for an unreadable bucket (§16.4) — see this task's report for why
// that choice was made for restore's digest/refusal cases too, rather than
// aborting the whole run on the first bad bucket.
func runRestoreBuckets(h *CommandHandler, opts restoreCLIOptions, progress io.Writer, interrupted <-chan struct{}) (*restoreSummary, error) {
	progress = inertWriter{progress}
	manifestPath := filepath.Join(opts.Dir, "manifest.json")
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("%q does not look like a hoardDB dump directory (reading manifest.json: %w)", opts.Dir, err)
	}
	var manifest topLevelManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return nil, fmt.Errorf("%q does not look like a hoardDB dump directory (parsing manifest.json: %w)", opts.Dir, err)
	}
	if !restorableFormatCLI(manifest.FormatVersion) {
		return nil, fmt.Errorf("manifest.json is format_version %d, this build restores format_versions 2 and %d. Upgrade hoarddb to a build that restores format_version %d",
			manifest.FormatVersion, dumpManifestFormatVersionCLI, manifest.FormatVersion)
	}

	summary := newRestoreSummary()
	summary.dryRun = opts.DryRun

	if manifest.Incomplete {
		if !opts.AllowIncomplete {
			return nil, fmt.Errorf("manifest.json is marked incomplete (%d bucket(s) did not dump cleanly) — pass --allow-incomplete to restore what is present anyway, or re-run dump first",
				len(manifest.Failures))
		}
		missing := make([]string, 0, len(manifest.Failures))
		for _, f := range manifest.Failures {
			missing = append(missing, fmt.Sprintf("%s/%s: %s", f.Database, f.Bucket, f.Error))
		}
		note := fmt.Sprintf("--allow-incomplete: this dump is missing %d bucket(s): %s", len(manifest.Failures), strings.Join(missing, "; "))
		summary.warnings = append(summary.warnings, note)
		_, _ = fmt.Fprintln(progress, "warning:", note)
	}

	// users.json must not be half-recreated (dump-restore-spec.md §5, this
	// task's brief): account restore is Task 4's territory, so its presence
	// is reported — visibly, in both the text summary and --json — and
	// otherwise not acted on at all, rather than attempting a partial
	// recreation.
	if _, err := os.Stat(filepath.Join(opts.Dir, "users.json")); err == nil {
		note := "users.json present but account restore is not yet implemented; accounts were not recreated"
		summary.warnings = append(summary.warnings, note)
		_, _ = fmt.Fprintln(progress, "warning:", note)
	}

	// Named before the first bucket is touched, not after: an operator who
	// learns which buckets came back empty only from the closing summary has
	// already pointed an application at them.
	blobBuckets, scanErr := blobBucketsMissingPayloads(opts.Dir)
	if scanErr != nil {
		_, _ = fmt.Fprintf(progress, "warning: could not fully check this dump for blob buckets: %v\n", scanErr)
	}
	if note := blobPayloadNote(blobConsequenceRestore, blobBuckets); note != "" {
		summary.warnings = append(summary.warnings, note)
		_, _ = fmt.Fprintln(progress, "warning:", note)
	}

	targets, err := resolveRestoreTargets(opts.Dir)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("nothing to restore: %q has no bucket metadata files", opts.Dir)
	}

	for _, target := range targets {
		if interrupted != nil {
			select {
			case <-interrupted:
				summary.interrupted = true
			default:
			}
		}
		if summary.interrupted {
			summary.incomplete = true
			summary.failures = append(summary.failures, dumpFailure{
				Database: target.Database, Bucket: target.Bucket, Error: "interrupted before this bucket started",
			})
			continue
		}

		outcome := restoreOneBucket(h, target, opts, progress, interrupted)
		if outcome.err != nil {
			if errors.Is(outcome.err, errDumpInterrupted) {
				summary.interrupted = true
			}
			summary.incomplete = true
			summary.failures = append(summary.failures, dumpFailure{
				Database: target.Database, Bucket: target.Bucket, Error: outcome.err.Error(),
			})
			continue
		}
		summary.addDatabase(target.Database)
		summary.restored = append(summary.restored, target.Database+"/"+target.Bucket)
		summary.buckets++
		summary.documentsApplied += outcome.applied
		summary.documentsSkipped += outcome.skipped
		summary.elementsSkipped += outcome.elementsSkipped
		summary.noteFactor(target, outcome.factor)
		if outcome.dryRun {
			_, _ = fmt.Fprintf(progress, "  %s/%s: %s\n", target.Database, target.Bucket, outcome.message)
		}
	}

	return summary, nil
}

// noteFactor records target when the start answer says its factor is not the
// one its dump records.
func (sm *restoreSummary) noteFactor(target restoreBucketTarget, factor restoreFactorAnswer) {
	note := factorNote{database: target.Database, bucket: target.Bucket, dumped: factor.dumped}
	switch {
	case factor.notRestored > 0:
		sm.factorNotRestored = append(sm.factorNotRestored, note)
	case factor.kept > 0:
		note.bucketed = factor.kept
		sm.factorKept = append(sm.factorKept, note)
	case factor.lowered > 0:
		note.bucketed = factor.lowered
		sm.factorLowered = append(sm.factorLowered, note)
	}
}

// resolveRestoreTargets walks opts.Dir for every
// `<database>/<bucket>.metadata.json`, the same layout dump writes
// (dump-restore-spec.md §4), sorted for a deterministic run order.
func resolveRestoreTargets(dir string) ([]restoreBucketTarget, error) {
	dbEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading --dir %q: %w", dir, err)
	}
	var targets []restoreBucketTarget
	for _, dbEntry := range dbEntries {
		if !dbEntry.IsDir() {
			continue
		}
		database := dbEntry.Name()
		bucketEntries, err := os.ReadDir(filepath.Join(dir, database))
		if err != nil {
			return nil, fmt.Errorf("reading database directory %q: %w", database, err)
		}
		for _, be := range bucketEntries {
			name := be.Name()
			if be.IsDir() || !strings.HasSuffix(name, ".metadata.json") {
				continue
			}
			targets = append(targets, restoreBucketTarget{
				Database: database,
				Bucket:   strings.TrimSuffix(name, ".metadata.json"),
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Database != targets[j].Database {
			return targets[i].Database < targets[j].Database
		}
		return targets[i].Bucket < targets[j].Bucket
	})
	return targets, nil
}

// validNameComponentForRestore rejects a database or bucket name that is
// unsafe as a single path component (dump-restore-spec.md §9.1, §12.12):
// empty, a NUL byte, an absolute path, a path separator, or "."/"..". This
// is the CLI's own copy of server/restore.go's validRestoreNameComponent —
// duplicated rather than imported because it is unexported there (the same
// reason this file already keeps its own copy of the manifest
// format-version constant, dumpManifestFormatVersionCLI) — checked on the
// read side (--dir) the same way the server checks it on the write side
// (the data directory).
func validNameComponentForRestore(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s name is empty", kind)
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("%s name %q contains a NUL byte", kind, name)
	case filepath.IsAbs(name):
		return fmt.Errorf("%s name %q is an absolute path", kind, name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("%s name %q contains a path separator", kind, name)
	case name == "." || name == "..":
		return fmt.Errorf("%s name %q is a reserved path component", kind, name)
	}
	return nil
}

// validateRestoreReadPath canonicalises the join of --dir with database and
// bucket and asserts the result stays inside --dir (dump-restore-spec.md
// §9.1, §19 A-12). The server no longer needs a write-side twin: on the shared
// layout a bucket name is a key, never a path (server/restore.go's
// validateBucketName), but --dir is a real directory the CLI reads.
func validateRestoreReadPath(dir, database, bucket string) error {
	if err := validNameComponentForRestore("database", database); err != nil {
		return err
	}
	if err := validNameComponentForRestore("bucket", bucket); err != nil {
		return err
	}
	root := filepath.Clean(dir)
	joined := filepath.Clean(filepath.Join(dir, database, bucket))
	if joined != root && !strings.HasPrefix(joined, root+string(filepath.Separator)) {
		return fmt.Errorf("database %q / bucket %q canonicalises to %q, outside --dir %q", database, bucket, joined, root)
	}
	return nil
}

// readStoresFile reads and checks a format-2 bucket's .stores.bson — its
// digest and its element count against metadata.json — before a byte is
// sent: a bucket whose metadata records elements and whose file is missing or
// corrupt is refused, never restored without its store fields.
func readStoresFile(path string, meta bucketMetadataFile) ([][]byte, error) {
	if meta.Elements == 0 {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s (metadata.json records %d store-field element(s)): %w", path, meta.Elements, err)
	}
	sum := sha256.Sum256(raw)
	if digest := "sha256:" + hex.EncodeToString(sum[:]); digest != meta.StoresDigest {
		return nil, fmt.Errorf("%s: digest mismatch (file is %s, metadata.json says %s) — the .stores.bson file may be corrupt or truncated", path, digest, meta.StoresDigest)
	}
	elements, err := splitDumpFileIntoDocuments(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if int64(len(elements)) != meta.Elements {
		return nil, fmt.Errorf("%s: metadata.json says %d element(s) but the file actually contains %d", path, meta.Elements, len(elements))
	}
	return elements, nil
}

// readDeadFile reads and checks a format-3 bucket's .dead.bson — its digest
// and its count against metadata.json — before a byte is sent: a bucket whose
// metadata records dead entries and whose file is missing or corrupt is
// refused, never restored without them. A bucket that records none must not
// have a dead file either way: the file is not looked for.
func readDeadFile(path string, meta bucketMetadataFile) ([][]byte, error) {
	if meta.Dead == 0 {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s (metadata.json records %d dead entries): %w", path, meta.Dead, err)
	}
	sum := sha256.Sum256(raw)
	if digest := "sha256:" + hex.EncodeToString(sum[:]); digest != meta.DeadDigest {
		return nil, fmt.Errorf("%s: digest mismatch (file is %s, metadata.json says %s) — the .dead.bson file may be corrupt or truncated", path, digest, meta.DeadDigest)
	}
	entries, err := splitDumpFileIntoDocuments(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if int64(len(entries)) != meta.Dead {
		return nil, fmt.Errorf("%s: metadata.json says %d dead entries but the file actually contains %d", path, meta.Dead, len(entries))
	}
	return entries, nil
}

// splitDumpFileIntoDocuments splits a concatenation of whole BSON documents
// (a `<bucket>.bson` file) back into individual document byte slices, the
// same self-describing 4-byte length-prefix framing
// server/restore.go's parseBSONDocuments (and server/dump_test.go's
// splitBSONDocuments, which checks the dump side of this exact contract)
// rely on.
func splitDumpFileIntoDocuments(data []byte) ([][]byte, error) {
	var docs [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("trailing %d byte(s) are not a full BSON length prefix", len(data))
		}
		n := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
		if n < 5 || n > len(data) {
			return nil, fmt.Errorf("invalid BSON document length %d in %d remaining byte(s)", n, len(data))
		}
		docs = append(docs, data[:n])
		data = data[n:]
	}
	return docs, nil
}

// packRestoreChunks concatenates whole documents into chunks up to
// chunkSize each, never splitting one document across two chunks — the
// same rule OpDumpChunk's responses follow, mirrored here on the way back
// in so a single oversized-but-still-frameable document can make one chunk
// larger than the nominal target, exactly as it can on dump's side.
func packRestoreChunks(docs [][]byte, chunkSize int) [][]byte {
	var chunks [][]byte
	var current []byte
	for _, doc := range docs {
		if len(current) > 0 && len(current)+len(doc) > chunkSize {
			chunks = append(chunks, current)
			current = nil
		}
		current = append(current, doc...)
		if len(current) >= chunkSize {
			chunks = append(chunks, current)
			current = nil
		}
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}

// bucketRestoreOutcome is one bucket's restore result.
type bucketRestoreOutcome struct {
	applied int64
	skipped int64
	// elementsSkipped is the store-field elements the server counted and did not write.
	elementsSkipped int64
	created         bool
	dropped         bool
	dryRun          bool
	message         string
	factor          restoreFactorAnswer
	err             error
}

// restoreFactorAnswer is what a restore start said about the bucket's factor.
type restoreFactorAnswer struct {
	dumped, notRestored, kept, lowered int
}

// factorAnswerOf reads the factor half of a start answer for a dump that
// records dumped.
func factorAnswerOf(dumped int, start server.RestoreStartResponse) restoreFactorAnswer {
	return restoreFactorAnswer{dumped: dumped, notRestored: start.FactorNotRestored, kept: start.FactorKept, lowered: start.FactorLowered}
}

// restoreOneBucket validates one bucket's dumped files (path safety, the
// reserved namespace, format version, the per-file digest, the declared
// document count) before sending a single byte to the server, then drives
// OpRestoreStart/Chunk/Done — or, with opts.DryRun, only OpRestoreStart,
// which validates everything server-side and reports what would change
// without registering a session (server/restore.go's dryRunRestoreStart).
func restoreOneBucket(h *CommandHandler, target restoreBucketTarget, opts restoreCLIOptions, progress io.Writer, interrupted <-chan struct{}) bucketRestoreOutcome {
	progress = inertWriter{progress}
	if err := validateRestoreReadPath(opts.Dir, target.Database, target.Bucket); err != nil {
		return bucketRestoreOutcome{err: err}
	}
	if server.IsReservedDatabaseName(target.Database) {
		return bucketRestoreOutcome{err: fmt.Errorf("database %q is reserved for internal use and cannot be restored into", target.Database)}
	}

	dbDir := filepath.Join(opts.Dir, target.Database)
	metaPath := filepath.Join(dbDir, target.Bucket+".metadata.json")
	bsonPath := filepath.Join(dbDir, target.Bucket+".bson")

	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("reading %s: %w", metaPath, err)}
	}
	var meta bucketMetadataFile
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("parsing %s: %w", metaPath, err)}
	}
	if !restorableFormatCLI(meta.FormatVersion) {
		return bucketRestoreOutcome{err: fmt.Errorf("%s: format_version %d, this build restores format_versions 2 and %d. Upgrade hoarddb to a build that restores format_version %d", metaPath, meta.FormatVersion, dumpManifestFormatVersionCLI, meta.FormatVersion)}
	}

	bsonRaw, err := os.ReadFile(bsonPath)
	if err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("reading %s: %w", bsonPath, err)}
	}
	// Per-file digest, verified before a single byte reaches the server
	// (dump-restore-spec.md §4, §9.1, §12.8): a bit-flip in the .bson file
	// refuses this bucket without ever calling OpRestoreStart.
	sum := sha256.Sum256(bsonRaw)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if meta.Digest != "" && digest != meta.Digest {
		return bucketRestoreOutcome{err: fmt.Errorf("%s: digest mismatch (file is %s, metadata.json says %s) — the .bson file may be corrupt or truncated", bsonPath, digest, meta.Digest)}
	}

	docs, err := splitDumpFileIntoDocuments(bsonRaw)
	if err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("%s: %w", bsonPath, err)}
	}
	if int64(len(docs)) != meta.Documents {
		return bucketRestoreOutcome{err: fmt.Errorf("%s: metadata.json says %d document(s) but the file actually contains %d", bsonPath, meta.Documents, len(docs))}
	}
	elements, err := readStoresFile(filepath.Join(dbDir, target.Bucket+".stores.bson"), meta)
	if err != nil {
		return bucketRestoreOutcome{err: err}
	}
	deadEntries, err := readDeadFile(filepath.Join(dbDir, target.Bucket+".dead.bson"), meta)
	if err != nil {
		return bucketRestoreOutcome{err: err}
	}

	startReq := server.RestoreStartRequest{
		Database:      target.Database,
		Bucket:        target.Bucket,
		FormatVersion: meta.FormatVersion,
		StoreType:     server.BucketType(meta.StoreType),
		Indexes:       meta.Indexes,
		IndexSpecs:    meta.IndexSpecs,
		Stores:        meta.Stores,
		Bytes:         meta.Bytes + meta.StoresBytes + meta.DeadBytes,
		Dead:          meta.Dead,
		Drop:          opts.Drop,
		DryRun:        opts.DryRun,

		ReplicationFactor: meta.ReplicationFactor,
		Placement:         meta.Placement,
	}
	var startResp server.RestoreStartResponse
	if err := h.send(transport.OpRestoreStart, &startReq, &startResp); err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("restore start: %w", err)}
	}
	if opts.DryRun {
		return bucketRestoreOutcome{dryRun: true, created: startResp.Created, dropped: startResp.Dropped, message: startResp.Message,
			factor: factorAnswerOf(meta.ReplicationFactor, startResp)}
	}

	_, _ = fmt.Fprintf(progress, "restoring %s/%s (%d document(s))...\n", target.Database, target.Bucket, len(docs))

	// Documents first, then their store fields' elements: an element whose
	// document has not arrived is refused as having no parent.
	chunks := packRestoreChunks(docs, transport.DumpChunkSize)
	sections := make([]string, len(chunks))
	for _, chunk := range packRestoreChunks(elements, transport.DumpChunkSize) {
		chunks = append(chunks, chunk)
		sections = append(sections, "stores")
	}
	// The dead entries go last, after every document and element: they are
	// written back as dead entries, not pushed.
	for _, chunk := range packRestoreChunks(deadEntries, transport.DumpChunkSize) {
		chunks = append(chunks, chunk)
		sections = append(sections, "dead")
	}
	for i, chunk := range chunks {
		if interrupted != nil {
			select {
			case <-interrupted:
				abortRestore(h, target, startResp.SessionID)
				return bucketRestoreOutcome{err: errDumpInterrupted}
			default:
			}
		}
		var chunkResp server.RestoreChunkResponse
		if err := h.send(transport.OpRestoreChunk, &server.RestoreChunkRequest{
			Database: target.Database, Bucket: target.Bucket, SessionID: startResp.SessionID, Data: chunk, Section: sections[i],
		}, &chunkResp); err != nil {
			abortRestore(h, target, startResp.SessionID)
			return bucketRestoreOutcome{err: fmt.Errorf("restore chunk: %w", err)}
		}
	}

	var doneResp server.RestoreDoneResponse
	if err := h.send(transport.OpRestoreDone, &server.RestoreDoneRequest{
		Database: target.Database, Bucket: target.Bucket, SessionID: startResp.SessionID, Documents: meta.Documents, Elements: meta.Elements, Dead: meta.Dead,
	}, &doneResp); err != nil {
		return bucketRestoreOutcome{err: fmt.Errorf("restore done: %w", err)}
	}
	if !doneResp.Success {
		return bucketRestoreOutcome{err: fmt.Errorf("%s", doneResp.Message)}
	}

	_, _ = fmt.Fprintf(progress, "  %s/%s: %d applied, %d skipped\n", target.Database, target.Bucket, doneResp.Applied, doneResp.Skipped)
	if meta.Elements > 0 {
		_, _ = fmt.Fprintf(progress, "  %s/%s store fields: %d element(s) applied, %d skipped\n", target.Database, target.Bucket, doneResp.ElementsApplied, doneResp.ElementsSkipped)
	}
	if meta.Dead > 0 {
		_, _ = fmt.Fprintf(progress, "  %s/%s: %d dead entries restored as dead\n", target.Database, target.Bucket, doneResp.DeadApplied)
	}
	return bucketRestoreOutcome{applied: doneResp.Applied, skipped: doneResp.Skipped, elementsSkipped: doneResp.ElementsSkipped, created: startResp.Created, dropped: startResp.Dropped,
		factor: factorAnswerOf(meta.ReplicationFactor, startResp)}
}
