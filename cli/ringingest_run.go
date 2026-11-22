package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/server"
)

// ringDefaultBatch is the default --batch: documents per RING_BATCH frame.
const ringDefaultBatch = 100

const ringIngestUsageText = `Usage: hoarddb ingest <file> --into <database>.<bucket> [flags]

Loads a BSON file (.bson) or a JSON Lines file (.jsonl, one document per line) into one
bucket, creating the database and the bucket if they do not exist. The file is read twice:
once to work out its fields and their types, once to write, so it must be a file and not
standard input. Running the same command again changes nothing that is already loaded: every
document is written as an upsert by its _id.

Ids: the ring's _id_ is text. An ObjectId becomes its 24 lowercase hex digits and a string
stays as it is; any other type gets a prefix, for example the integer 1 becomes ~i:1.

Connection: -address, -user and -password say where and who; pass -fingerprint SHA256:<hex>
(the server prints it when it starts) or, for a node you started yourself, -insecure.

Examples:
  hoarddb ingest users.bson --into app.users
  hoarddb ingest users.bson --into app.users --dry-run
  hoarddb ingest events.jsonl --into app.events --on-error skip --rejects events.rejects.jsonl

Flags:
`

// removedIngestFlags are the flags of the legacy ingest that the ring port does not have, each
// with the sentence that says what to do instead (AGENTS.md rule 3). There is no alias.
var removedIngestFlags = map[string]string{
	"type":              "--type is gone: a bucket has no type. Declare what it holds with 'create bucket <name> in <db> { <Field> <type> }', then load the file with --into <database>.<bucket>.",
	"id-as":             "--id-as is gone: an _id is rendered by one fixed rule, an ObjectId as its 24 hex digits and any other type with a prefix such as ~i:1. Nothing to pass; 'hoarddb ingest --help' lists the rule.",
	"write-concern":     "--write-concern is gone: the ring decides how many copies confirm a write from the database's replication factor. Drop the flag.",
	"allow-expiring":    "--allow-expiring is gone: there is no time-to-live index to guard. A bucket that declares 'expire at <Field>' expires by that field's date, which the file supplies. Drop the flag.",
	"state":             "--state is gone: ingest keeps no resume file. Run the same command again; every document is an upsert, so what is already loaded is not added twice.",
	"no-resume":         "--no-resume is gone: ingest keeps no resume file. Run the same command again; every document is an upsert, so what is already loaded is not added twice.",
	"resume-anyway":     "--resume-anyway is gone: ingest keeps no resume file. Run the same command again; every document is an upsert, so what is already loaded is not added twice.",
	"accept-duplicates": "--accept-duplicates is gone: a document is an upsert by its _id, so a second run cannot add it twice. Drop the flag.",
	"from-offset":       "--from-offset is gone: ingest keeps no resume point. Run the same command again, or cut the file at the document you want to start from.",
	"again":             "--again is gone: ingest keeps no record of finished files. Run the same command again; it is safe.",
	"checkpoint-every":  "--checkpoint-every is gone: progress is printed on its own. Drop the flag.",
	"into-db":           ringIntoDBText,
	"max-buckets":       "--max-buckets is gone: it bounded a directory load. " + ringIntoDBText,
	"allow-dots":        "--allow-dots is gone: it belonged to a directory load. " + ringIntoDBText,
}

const (
	ringIntoDBText = "Loading a whole directory is not available. Load each file into its bucket: hoarddb ingest <dir>/<name>.bson --into <database>.<name>"
	ringStoreField = "Loading a file into a store field of other documents is not available. Load the file into a bucket of its own with --into <database>.<bucket>, or push its elements from a driver."
)

// ringIngestEnv is what the verb touches outside its arguments, so a test can run it without a
// process.
type ringIngestEnv struct {
	stdout, stderr   io.Writer
	stderrIsTerminal bool
}

// ringIngestFlags are the parsed flags and the one positional argument.
type ringIngestFlags struct {
	connection *connectionFlags
	flagSet    *flag.FlagSet
	input      string
	into       string
	format     string
	noCreate   bool
	onError    string
	rejects    string
	batch      int
	dryRun     bool
	jsonOut    bool
}

// RunRingIngestCommand implements the standalone 'hoarddb ingest' verb on the ring: one BSON
// or JSON Lines file into one bucket. Progress and every message go to stderr; the exit code
// is the shared table of cli/exitcodes.go.
func RunRingIngestCommand(args []string) int {
	installClientLogger(false)
	// SIGINT and SIGTERM end the run alike; see verbStopContext.
	ctx, stop := verbStopContext()
	defer stop()
	return runRingIngest(ctx, args, ringIngestEnv{stdout: os.Stdout, stderr: os.Stderr, stderrIsTerminal: isTerminalFile(os.Stderr) && terminalCanTakeEscapes()})
}

// refuseRingIngest prints a failure and returns its exit code. With --json it also prints the
// result object, so a script reading stdout always gets one.
func refuseRingIngest(env ringIngestEnv, flags *ringIngestFlags, err error) int {
	_, _ = fmt.Fprintf(env.stderr, "ingest: %s\n", terminalSafeText(err.Error()))
	if flags != nil && flags.jsonOut {
		report := newRingIngestReport(filepath.Base(flags.input), formatFromName(flags), flags.dryRun)
		report.StoppedBecause = err.Error()
		writeRingIngestJSON(env, report)
	}
	var usage *ingestUsageError
	var refusal *ingestRefusal
	var unknown *ringOutcomeUnknownError
	switch {
	case errors.As(err, &usage):
		return ExitUsage
	case errors.As(err, &refusal):
		return ExitRefused
	case errors.As(err, &unknown):
		return ExitUnavailable
	}
	return ringFailure(err).Exit
}

// formatFromName is the format a refusal before the file was opened can still name: --format
// when given, else what the file name says, else empty.
func formatFromName(flags *ringIngestFlags) string {
	lower := strings.ToLower(flags.input)
	switch {
	case flags.format != "":
		return flags.format
	case strings.HasSuffix(lower, ".bson"):
		return string(formatBSON)
	case strings.HasSuffix(lower, ".jsonl"), strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".json"):
		return string(formatJSONL)
	}
	return ""
}

// parseRingIngestArgs reads the flags, which may stand before or after the path (Go's flag
// package stops at the first positional, so it parses again after taking one).
func parseRingIngestArgs(args []string, env ringIngestEnv) (*ringIngestFlags, error) {
	for _, argument := range args {
		if argument == "--" {
			break
		}
		name, isFlag := strings.CutPrefix(argument, "-")
		name = strings.TrimPrefix(name, "-")
		name, _, _ = strings.Cut(name, "=")
		if text, removed := removedIngestFlags[name]; isFlag && removed {
			return nil, &ingestUsageError{Message: text}
		}
	}
	var usage strings.Builder
	fs := flag.NewFlagSet("hoarddb ingest", flag.ContinueOnError)
	fs.SetOutput(&usage)
	parsed := &ringIngestFlags{connection: registerConnectionFlags(fs), flagSet: fs}
	// The ring pins a fingerprint (ringdial.go), which the legacy flag text still calls
	// unsupported.
	fs.Lookup("fingerprint").Usage = "pin the server's certificate fingerprint, SHA256:<64 hex>; the server prints it when it starts (or pass -insecure for a node you started yourself)"
	fs.StringVar(&parsed.into, "into", "", "target as <database>.<bucket> (required)")
	fs.StringVar(&parsed.format, "format", "", "bson or jsonl; overrides detection by file name")
	fs.BoolVar(&parsed.noCreate, "no-create", false, "refuse rather than create a missing database or bucket")
	fs.StringVar(&parsed.onError, "on-error", "stop", "stop (default) or skip a document that cannot be loaded")
	fs.StringVar(&parsed.rejects, "rejects", "", "with --on-error skip: write the skipped documents to this file (created 0600, never overwritten)")
	fs.IntVar(&parsed.batch, "batch", ringDefaultBatch, "documents per request, 1 to 255 (a 2 MiB byte budget applies as well)")
	fs.BoolVar(&parsed.dryRun, "dry-run", false, "read the file and say what would be created and written, but write nothing")
	fs.BoolVar(&parsed.jsonOut, "json", false, "print one JSON result object to stdout as well as the text summary on stderr")

	var positionals []string
	remaining := args
	for {
		if err := fs.Parse(remaining); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				_, _ = io.WriteString(env.stdout, ringIngestUsageText)
				fs.SetOutput(env.stdout)
				fs.PrintDefaults()
				return nil, flag.ErrHelp
			}
			return nil, &ingestUsageError{Message: fmt.Sprintf("%s. Run 'hoarddb ingest --help' for the flags and examples.", firstLine(usage.String(), err))}
		}
		remaining = fs.Args()
		if len(remaining) == 0 {
			break
		}
		positionals = append(positionals, remaining[0])
		remaining = remaining[1:]
	}
	if len(positionals) != 1 {
		return nil, &ingestUsageError{Message: fmt.Sprintf("ingest takes exactly one file, and got %d. Example: hoarddb ingest users.bson --into app.users", len(positionals))}
	}
	parsed.input = positionals[0]
	return parsed, nil
}

// validate checks what needs neither a connection nor the file, and splits --into.
func (f *ringIngestFlags) validate() (database, bucket string, err error) {
	switch {
	case f.into == "":
		return "", "", &ingestUsageError{Message: "--into <database>.<bucket> is required. Example: hoarddb ingest " + f.input + " --into app.users"}
	case strings.Count(f.into, ".") > 1:
		return "", "", &ingestUsageError{Message: ringStoreField}
	}
	database, bucket, err = splitInto(f.into)
	if err != nil {
		return "", "", err
	}
	if server.IsReservedDatabaseName(database) {
		return "", "", &ingestUsageError{Message: fmt.Sprintf("%q is reserved for the server's own use and cannot be an ingest target. Choose another database, for example --into app.%s", database, bucket)}
	}
	switch {
	case f.input == "-":
		return "", "", &ingestUsageError{Message: "ingest reads the file twice (once to work out the fields, once to write), so it cannot read standard input. Save it to a file first, then run: hoarddb ingest <file> --into " + f.into}
	case f.format != "" && f.format != string(formatBSON) && f.format != string(formatJSONL):
		return "", "", &ingestUsageError{Message: fmt.Sprintf("--format %q is not a format ingest reads. Use --format bson or --format jsonl", f.format)}
	case f.onError != "stop" && f.onError != "skip":
		return "", "", &ingestUsageError{Message: fmt.Sprintf("--on-error %q must be stop or skip", f.onError)}
	case f.rejects != "" && f.onError != "skip":
		return "", "", &ingestUsageError{Message: "--rejects saves the documents --on-error skip skips, so it needs --on-error skip"}
	case f.batch < 1 || f.batch > ringMaxBatchDocuments:
		return "", "", &ingestUsageError{Message: fmt.Sprintf("--batch is %d and a request carries 1 to %d documents. Example: --batch 100", f.batch, ringMaxBatchDocuments)}
	}
	return database, bucket, nil
}

// ringIngestInput is the file being loaded.
type ringIngestInput struct {
	path, name string
	size       int64
	format     inputFormat
}

// openRingIngestInput checks the path is a readable file and decides its format from its name
// and first bytes.
func openRingIngestInput(f *ringIngestFlags) (*ringIngestInput, error) {
	info, err := os.Stat(f.input)
	switch {
	case err != nil:
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot read %s: %v. Check the path.", f.input, err)}
	case info.IsDir():
		return nil, &ingestUsageError{Message: ringIntoDBText}
	}
	file, err := os.Open(f.input)
	if err != nil {
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot read %s: %v. Check the path and its permissions.", f.input, err)}
	}
	defer func() { _ = file.Close() }()
	first := make([]byte, 4096)
	count, err := io.ReadFull(file, first)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot read %s: %v", f.input, err)}
	}
	format, err := detectFormat(f.input, first[:count], f.format)
	if err != nil {
		return nil, &ingestUsageError{Message: err.Error()}
	}
	return &ringIngestInput{path: f.input, name: filepath.Base(f.input), size: info.Size(), format: format}, nil
}

// open returns a reader on the file from its start; pass 1 and pass 2 each call it.
func (in *ringIngestInput) open(ctx context.Context) func() (documentReader, io.Closer, error) {
	return func() (documentReader, io.Closer, error) {
		file, err := os.Open(in.path)
		if err != nil {
			return nil, nil, fmt.Errorf("opening %s: %w", in.path, err)
		}
		start := readerStart{name: in.name, totalBytes: in.size}
		var reader documentReader
		if in.format == formatBSON {
			reader = newBSONReader(file, start)
		} else {
			reader = newJSONLReader(file, start)
		}
		return stoppableReader{ctx: ctx, reader: reader}, file, nil
	}
}

// stoppableReader ends a read at the next document once the run is cancelled, so a Ctrl-C
// during the first pass does not wait for the whole file.
type stoppableReader struct {
	ctx    context.Context
	reader documentReader
}

// Next implements documentReader.
func (r stoppableReader) Next() (readDocument, error) {
	if err := r.ctx.Err(); err != nil {
		return readDocument{}, fmt.Errorf("stopped: %w", err)
	}
	return r.reader.Next()
}

// ringRejects receives the raw bytes of every document --on-error skip skips, so a user can
// fix and load just those.
type ringRejects struct {
	file    *os.File
	path    string
	jsonl   bool
	written int
}

// createRingRejects creates path with mode 0600 and refuses to overwrite it.
func createRingRejects(path string, jsonl bool) (*ringRejects, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, &ingestUsageError{Message: fmt.Sprintf("%s already exists, and ingest will not overwrite a rejects file. Delete it or pass a different --rejects path.", path)}
		}
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot create the rejects file %s: %v. Pass a writable --rejects path, or drop --rejects.", path, err)}
	}
	return &ringRejects{file: file, path: path, jsonl: jsonl}, nil
}

// add appends one rejected document; a JSON line gets its newline back.
func (r *ringRejects) add(source []byte) error {
	if _, err := r.file.Write(source); err != nil {
		return fmt.Errorf("writing the rejects file %s: %w", r.path, err)
	}
	if r.jsonl {
		if _, err := r.file.Write([]byte("\n")); err != nil {
			return fmt.Errorf("writing the rejects file %s: %w", r.path, err)
		}
	}
	r.written++
	return nil
}

// finish closes the file and removes it when nothing was rejected, so a clean run leaves no
// empty file behind.
func (r *ringRejects) finish() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("closing the rejects file %s: %w", r.path, err)
	}
	if r.written == 0 {
		if err := os.Remove(r.path); err != nil {
			return fmt.Errorf("removing the empty rejects file %s: %w", r.path, err)
		}
	}
	return nil
}

// ringIngestRun is one invocation past argument parsing.
type ringIngestRun struct {
	env              ringIngestEnv
	out              io.Writer
	flags            *ringIngestFlags
	input            *ringIngestInput
	database, bucket string
	started          time.Time
	rejects          *ringRejects
}

// runRingIngest is the verb: usage checks and the file first (exit 1, no dial), then connect
// (6, 7), then pass 1, the target, pass 2 and the summary.
func runRingIngest(ctx context.Context, args []string, env ringIngestEnv) int {
	flags, err := parseRingIngestArgs(args, env)
	if errors.Is(err, flag.ErrHelp) {
		return ExitOK
	}
	if err != nil {
		return refuseRingIngest(env, nil, err)
	}
	database, bucket, err := flags.validate()
	if err != nil {
		return refuseRingIngest(env, flags, err)
	}
	input, err := openRingIngestInput(flags)
	if err != nil {
		return refuseRingIngest(env, flags, err)
	}
	run := &ringIngestRun{env: env, out: inertWriter{env.stderr}, flags: flags, input: input, database: database, bucket: bucket, started: time.Now()}
	if flags.rejects != "" {
		if run.rejects, err = createRingRejects(flags.rejects, input.format == formatJSONL); err != nil {
			return refuseRingIngest(env, flags, err)
		}
		defer func() {
			if finishErr := run.rejects.finish(); finishErr != nil {
				_, _ = fmt.Fprintf(env.stderr, "ingest: %v\n", finishErr)
			}
		}()
	}
	client, redial, exit := dialRingForVerb(ctx, env.stderr, flags.flagSet, flags.connection)
	if client == nil {
		if flags.jsonOut {
			report := newRingIngestReport(input.name, string(input.format), flags.dryRun)
			report.Database, report.Bucket = database, bucket
			report.StoppedBecause = fmt.Sprintf("could not open a session on the ring (exit %d); the message is on stderr", exit)
			writeRingIngestJSON(env, report)
		}
		return exit
	}
	defer func() { _ = client.Close() }()
	return run.execute(ctx, client, redial)
}

// ringIngestOutcome is what pass 2 did, for the summary.
type ringIngestOutcome struct {
	read, skipped int64
	stats         ringSendStats
	// stopped is why the run ended before the file did, empty when it ran to the end.
	stopped     string
	stopExit    int
	interrupted bool
	// unrecorded counts skipped documents that could not be saved to the rejects file.
	unrecorded int64
	// failures are the first skipped documents by position, for the report.
	failures []ringIngestFailure
}

func (r *ringIngestRun) execute(ctx context.Context, client *ringclient.Client, redial ringRedial) int {
	scan, err := scanFile(r.input.open(ctx))
	if err != nil {
		return r.failScan(ctx, err)
	}
	if scan.Refused > 0 && r.flags.onError == "stop" {
		return refuseRingIngest(r.env, r.flags, &ingestRefusal{Message: fmt.Sprintf("%s Nothing was written. Fix the document, or pass --on-error skip to load the others and list the skipped ones in a file (--rejects).",
			scan.Refusals[0].text(r.input.name))})
	}
	params := ringIngestParams{Database: r.database, Bucket: r.bucket, File: r.input.name, NoCreate: r.flags.noCreate, DryRun: r.flags.dryRun}
	var prepared preparedTarget
	if scan.Documents > 0 {
		if prepared, err = prepareTarget(ctx, ringClientTarget{client}, params, scan); err != nil {
			return refuseRingIngest(r.env, r.flags, err)
		}
	}
	if r.flags.dryRun {
		r.printDryRun(scan, prepared)
		return ExitOK
	}
	sender := newRingBatchSender(client, redial, r.database, r.bucket, r.flags.batch)
	defer sender.close()
	outcome := r.write(ctx, scan, sender)
	return r.finish(scan, prepared, outcome)
}

// failScan turns an error of pass 1 into its exit code.
func (r *ringIngestRun) failScan(ctx context.Context, err error) int {
	var refusal fileRefusal
	var corrupt *inputCorruptError
	switch {
	case ctx.Err() != nil:
		_, _ = fmt.Fprintf(r.out, "ingest: interrupted while reading %s; nothing was written.\n", r.input.name)
		return ExitInterrupted
	case errors.As(err, &refusal):
		return refuseRingIngest(r.env, r.flags, &ingestRefusal{Message: refusal.fileText(r.input.name)})
	case errors.As(err, &corrupt):
		return refuseRingIngest(r.env, r.flags, &ingestUsageError{Message: fmt.Sprintf("%s cannot be read past byte %s: %s. Nothing was written. Re-export the file, or cut it at the last good document; --on-error skip does not help, because there is no next document to skip to.", r.input.name, formatCount(corrupt.Offset), strings.TrimSuffix(corrupt.Message, "."))})
	}
	return refuseRingIngest(r.env, r.flags, &ingestUsageError{Message: fmt.Sprintf("reading %s: %v", r.input.name, err)})
}

// write is pass 2: it reads the file again and sends every document that pass 1 accepted.
func (r *ringIngestRun) write(ctx context.Context, scan scanResult, sender *ringBatchSender) (outcome ringIngestOutcome) {
	// A named result, so the deferred copy of the sender's counters reaches the caller.
	defer func() { outcome.stats = sender.stats }()
	reader, closer, err := r.input.open(ctx)()
	if err != nil {
		outcome.stopped, outcome.stopExit = err.Error(), ExitUsage
		return outcome
	}
	defer func() { _ = closer.Close() }()
	// The progress line rewrites itself with control characters, so it does not go through
	// the writer that makes names inert.
	progress := newRingProgress(r.env.stderr, r.env.stderrIsTerminal, r.input.name, r.database+"."+r.bucket, scan.Documents+scan.Refused)
	defer progress.done()

	for outcome.stopped == "" {
		next, err := reader.Next()
		var failure *documentFailure
		switch {
		case errors.Is(err, io.EOF):
			r.sendRemainder(ctx, &outcome, sender)
			return outcome
		case ctx.Err() != nil:
			outcome.interrupted = true
			r.sendRemainder(ctx, &outcome, sender)
			return outcome
		case errors.As(err, &failure):
			outcome.read++
			r.skip(&outcome, failure.Ordinal, scanRefusal{Ordinal: failure.Ordinal, Err: failure}.text(r.input.name), failure.Source)
			continue
		case err != nil:
			outcome.stopped, outcome.stopExit = fmt.Sprintf("reading %s: %v", r.input.name, err), ExitUsage
			continue
		}
		outcome.read++
		id, err := ingestID(next.Doc)
		if err != nil {
			r.skip(&outcome, next.Ordinal, ingestIDRefusalText(err, next.Ordinal, r.input.name), next.Source)
			continue
		}
		doc, err := newRingIngestDoc(next, id)
		if err != nil {
			r.skip(&outcome, next.Ordinal, fmt.Sprintf("Document %s of %s cannot be read: %v.", formatCount(next.Ordinal), r.input.name, err), next.Source)
			continue
		}
		failures, sendErr := sender.add(ctx, doc)
		r.skipRefused(&outcome, failures)
		if sendErr != nil {
			r.stopOnSendError(ctx, &outcome, sendErr)
		}
		progress.update(outcome.read)
	}
	return outcome
}

// sendRemainder sends the frame still queued when the file ends or the run is interrupted.
func (r *ringIngestRun) sendRemainder(ctx context.Context, outcome *ringIngestOutcome, sender *ringBatchSender) {
	failures, err := sender.flush(ctx)
	r.skipRefused(outcome, failures)
	if err != nil {
		r.stopOnSendError(ctx, outcome, err)
	}
}

// skipRefused applies --on-error to the documents of a frame that the ring refused.
func (r *ringIngestRun) skipRefused(outcome *ringIngestOutcome, failures []ringDocFailure) {
	for _, failure := range failures {
		r.skip(outcome, failure.Doc.Ordinal, failure.text(r.input.name), failure.Doc.Source)
	}
}

// stopOnSendError ends the run on a refusal of a whole frame or an unknown outcome. A send
// that failed because a signal cancelled the run is an interruption (exit 4), not an outage:
// the last frame may or may not have arrived, and running the same command again is safe
// because every document is an upsert by _id.
func (r *ringIngestRun) stopOnSendError(ctx context.Context, outcome *ringIngestOutcome, err error) {
	if ctx.Err() != nil {
		outcome.interrupted = true
		return
	}
	var unknown *ringOutcomeUnknownError
	if errors.As(err, &unknown) {
		outcome.stopped = fmt.Sprintf("%s. Run the same command again: every document is an upsert, so the ones that did arrive are not added twice.", terminalSafeText(unknown.Error()))
		outcome.stopExit = ExitUnavailable
		return
	}
	failure := ringFailure(err)
	outcome.stopped, outcome.stopExit = failure.Message, failure.Exit
}

// skip applies --on-error to one document: under stop it ends the run naming the document,
// under skip it counts the document and saves its bytes to the rejects file.
func (r *ringIngestRun) skip(outcome *ringIngestOutcome, ordinal int64, text string, source []byte) {
	if outcome.stopped != "" {
		return
	}
	if r.flags.onError == "stop" {
		outcome.stopped = text + " Documents before it were loaded; fix it and run the same command again, or pass --on-error skip."
		outcome.stopExit = ExitRefused
		return
	}
	outcome.skipped++
	if len(outcome.failures) < ringReportedFailures {
		outcome.failures = append(outcome.failures, ringIngestFailure{Document: ordinal, Reason: text})
	}
	if r.rejects == nil {
		return
	}
	if source == nil {
		outcome.unrecorded++
		return
	}
	if err := r.rejects.add(source); err != nil {
		outcome.stopped, outcome.stopExit = err.Error(), ExitUsage
	}
}

// newRingIngestDoc turns a document of the file into what the sender writes: every top-level
// member except _id as a scalar field.
func newRingIngestDoc(next readDocument, id string) (ringIngestDoc, error) {
	elements, err := next.Doc.Elements()
	if err != nil {
		return ringIngestDoc{}, fmt.Errorf("reading its fields: %w", err)
	}
	fields := make(ringclient.Doc, len(elements))
	for _, element := range elements {
		if element.Key() != "_id" {
			fields[element.Key()] = element.Value()
		}
	}
	return ringIngestDoc{Ordinal: next.Ordinal, ID: id, Fields: fields, Source: next.Source, Bytes: len(next.Doc)}, nil
}

// finish prints the summary and returns the exit code.
func (r *ringIngestRun) finish(scan scanResult, prepared preparedTarget, outcome ringIngestOutcome) int {
	written := outcome.stats.Applied + outcome.stats.Deduped
	elapsed := time.Since(r.started)
	into := r.database + "." + r.bucket
	line := fmt.Sprintf("ingest: %s → %s — %s read, %s written", r.input.name, into, formatCount(outcome.read), formatCount(written))
	if outcome.skipped > 0 {
		line += fmt.Sprintf(", %s skipped", formatCount(outcome.skipped))
	}
	rate := ""
	if elapsed > time.Second && written > 0 {
		rate = fmt.Sprintf(" (%s docs/s)", formatCount(int64(float64(written)/elapsed.Seconds())))
	}
	_, _ = fmt.Fprintf(r.out, "%s in %s%s\n", line, formatDuration(elapsed), rate)
	if scan.Documents == 0 && scan.Refused == 0 {
		_, _ = fmt.Fprintf(r.out, "  %s has no documents, so no bucket was created\n", r.input.name)
	}
	if prepared.CreatedDatabase {
		_, _ = fmt.Fprintf(r.out, "  created database %s\n", r.database)
	}
	if prepared.CreatedBucket {
		_, _ = fmt.Fprintf(r.out, "  created bucket %s with %s\n", into, describeDeclaration(prepared.Declaration))
	}
	if prepared.Existing {
		_, _ = fmt.Fprintf(r.out, "  bucket %s exists and keeps its declaration: %s\n", into, describeDeclaration(prepared.Declaration))
	}
	for _, note := range prepared.Notes {
		_, _ = fmt.Fprintf(r.out, "  %s\n", note)
	}
	r.printDuplicates(scan)
	if outcome.skipped > 0 {
		r.printSkipped(outcome)
	}
	if outcome.stats.Resends > 0 || outcome.stats.Halvings > 0 {
		_, _ = fmt.Fprintf(r.out, "  %d resend(s) after a lost reply or a slot that was moving, and %d frame(s) cut in half; no document was added twice\n", outcome.stats.Resends, outcome.stats.Halvings)
	}
	code := ExitOK
	switch {
	case outcome.interrupted:
		_, _ = fmt.Fprintf(r.out, "ingest: interrupted after %s documents. Run the same command again to finish: every document is an upsert, so nothing is added twice.\n", formatCount(outcome.read))
		code = ExitInterrupted
	case outcome.stopped != "":
		_, _ = fmt.Fprintf(r.out, "ingest: stopped: %s\n", outcome.stopped)
		code = outcome.stopExit
	case outcome.skipped > 0:
		code = ExitPartial
	}
	if r.flags.jsonOut {
		writeRingIngestJSON(r.env, r.reportOf(scan, prepared, outcome, code))
	}
	return code
}

// reportOf is the JSON object of a finished run.
func (r *ringIngestRun) reportOf(scan scanResult, prepared preparedTarget, outcome ringIngestOutcome, code int) *ringIngestReport {
	report := newRingIngestReport(r.input.name, string(r.input.format), false)
	report.Database, report.Bucket = r.database, r.bucket
	report.OK = code == ExitOK
	report.Partial = !report.OK && outcome.stats.Applied+outcome.stats.Deduped > 0
	report.DocumentsRead = outcome.read
	report.DocumentsWritten = outcome.stats.Applied + outcome.stats.Deduped
	report.DocumentsSkipped = outcome.skipped
	report.CreatedDatabase, report.CreatedBucket = prepared.CreatedDatabase, prepared.CreatedBucket
	report.EmptyInput = scan.Documents == 0 && scan.Refused == 0
	report.Declaration = declarationReport(prepared.Declaration)
	report.Warnings = r.warnings(scan, prepared)
	report.Errors = append(report.Errors, outcome.failures...)
	report.ElapsedMS = time.Since(r.started).Milliseconds()
	switch {
	case outcome.interrupted:
		report.StoppedBecause = "interrupted"
	case outcome.stopped != "":
		report.StoppedBecause = outcome.stopped
	}
	return report
}

// warnings lists the sentences a script may want: the declaration's notes and the repeated ids.
func (r *ringIngestRun) warnings(scan scanResult, prepared preparedTarget) []string {
	warnings := append([]string{}, prepared.Notes...)
	for _, duplicate := range scan.Duplicates {
		warnings = append(warnings, duplicate.text(r.input.name))
	}
	return warnings
}

// ringReportedFailures bounds the skipped documents the JSON report names by position.
const ringReportedFailures = 100

// ringIngestFailure is one skipped document in the report: its position and why, never its
// content.
type ringIngestFailure struct {
	Document int64  `json:"document"`
	Reason   string `json:"reason"`
}

// ringIngestField is one declared field in the report.
type ringIngestField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ringIngestReport is the one object --json prints.
type ringIngestReport struct {
	OK               bool                `json:"ok"`
	Partial          bool                `json:"partial"`
	DryRun           bool                `json:"dry_run"`
	Input            string              `json:"input"`
	Format           string              `json:"format"`
	Database         string              `json:"database"`
	Bucket           string              `json:"bucket"`
	DocumentsRead    int64               `json:"documents_read"`
	DocumentsWritten int64               `json:"documents_written"`
	DocumentsSkipped int64               `json:"documents_skipped"`
	EmptyInput       bool                `json:"empty_input"`
	CreatedDatabase  bool                `json:"created_database"`
	CreatedBucket    bool                `json:"created_bucket"`
	Declaration      []ringIngestField   `json:"declaration"`
	Warnings         []string            `json:"warnings"`
	Errors           []ringIngestFailure `json:"errors"`
	ElapsedMS        int64               `json:"elapsed_ms"`
	StoppedBecause   string              `json:"stopped_because,omitempty"`
}

// newRingIngestReport starts a report whose lists are empty rather than null, so a script can
// iterate them.
func newRingIngestReport(input, format string, dryRun bool) *ringIngestReport {
	return &ringIngestReport{Input: input, Format: format, DryRun: dryRun, Declaration: []ringIngestField{}, Warnings: []string{}, Errors: []ringIngestFailure{}}
}

func declarationReport(fields []ringclient.FieldDecl) []ringIngestField {
	report := make([]ringIngestField, len(fields))
	for i, field := range fields {
		report[i] = ringIngestField{Name: field.Name, Type: field.Type}
	}
	return report
}

// writeRingIngestJSON prints the report as the one object on stdout.
func writeRingIngestJSON(env ringIngestEnv, report *ringIngestReport) {
	encoded, err := json.Marshal(report)
	if err != nil {
		_, _ = fmt.Fprintf(env.stderr, "ingest: cannot encode the --json result: %v\n", err)
		return
	}
	_, _ = fmt.Fprintln(env.stdout, string(encoded))
}

// describeDeclaration renders "2 fields: name string, age int".
func describeDeclaration(fields []ringclient.FieldDecl) string {
	if len(fields) == 0 {
		return "no fields"
	}
	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = field.Name + " " + field.Type
	}
	noun := "fields"
	if len(fields) == 1 {
		noun = "field"
	}
	return fmt.Sprintf("%d %s: %s", len(fields), noun, strings.Join(parts, ", "))
}

// printDuplicates says which documents of the file repeat an _id.
func (r *ringIngestRun) printDuplicates(scan scanResult) {
	shown := min(len(scan.Duplicates), 5)
	for _, duplicate := range scan.Duplicates[:shown] {
		_, _ = fmt.Fprintf(r.out, "  %s\n", duplicate.text(r.input.name))
	}
	if more := scan.DuplicateCount - int64(shown); more > 0 {
		_, _ = fmt.Fprintf(r.out, "  Warning: %s more documents repeat an _id of an earlier one; the later document of each pair is the one kept.\n", formatCount(more))
	}
	if scan.DuplicateCheckStopped {
		_, _ = fmt.Fprintf(r.out, "  Warning: repeated _ids were checked only in the first %s documents of the file.\n", formatCount(scanDuplicateIDLimit))
	}
}

// printSkipped says where the skipped documents went.
func (r *ringIngestRun) printSkipped(outcome ringIngestOutcome) {
	if r.rejects == nil {
		_, _ = fmt.Fprintf(r.out, "  %s skipped and not saved. Add --rejects <file> to keep them.\n", formatCount(outcome.skipped))
	} else {
		_, _ = fmt.Fprintf(r.out, "  %s skipped; the ones that could be saved are in %s\n", formatCount(outcome.skipped), r.rejects.path)
	}
	if outcome.unrecorded > 0 {
		_, _ = fmt.Fprintf(r.out, "  %s of them were too large to save to the rejects file.\n", formatCount(outcome.unrecorded))
	}
}

// printDryRun says what a run would do, creating nothing.
func (r *ringIngestRun) printDryRun(scan scanResult, prepared preparedTarget) {
	into := r.database + "." + r.bucket
	line := fmt.Sprintf("ingest (dry run): %s → %s — %s read, would write %s", r.input.name, into, formatCount(scan.Documents+scan.Refused), formatCount(scan.Documents))
	if scan.Refused > 0 {
		line += fmt.Sprintf(", would skip %s", formatCount(scan.Refused))
	}
	_, _ = fmt.Fprintln(r.out, line)
	if scan.Documents == 0 && scan.Refused == 0 {
		_, _ = fmt.Fprintf(r.out, "  %s has no documents, so no bucket would be created\n", r.input.name)
	}
	if prepared.WouldCreateDatabase {
		_, _ = fmt.Fprintf(r.out, "  would create database %s\n", r.database)
	}
	if prepared.Existing {
		_, _ = fmt.Fprintf(r.out, "  bucket %s exists and keeps its declaration: %s\n", into, describeDeclaration(prepared.Declaration))
	}
	if prepared.WouldCreateBucket {
		fields := prepared.Declaration
		noun := "fields"
		if len(fields) == 1 {
			noun = "field"
		}
		_, _ = fmt.Fprintf(r.out, "  would create bucket %s with %d %s:\n", into, len(fields), noun)
		for _, field := range fields {
			_, _ = fmt.Fprintf(r.out, "    %s %s\n", field.Name, field.Type)
		}
		_, _ = fmt.Fprintln(r.out, "  cost: a bucket declaration is 8 keys and 436 bytes plus about 26 bytes per field (computed, not measured)")
	}
	for _, note := range prepared.Notes {
		_, _ = fmt.Fprintf(r.out, "  %s\n", note)
	}
	r.printDuplicates(scan)
	_, _ = fmt.Fprintln(r.out, "  it read the file only, and nothing was written")
	if r.flags.jsonOut {
		report := newRingIngestReport(r.input.name, string(r.input.format), true)
		report.Database, report.Bucket = r.database, r.bucket
		report.OK = scan.Refused == 0
		report.EmptyInput = scan.Documents == 0 && scan.Refused == 0
		report.DocumentsRead, report.DocumentsWritten, report.DocumentsSkipped = scan.Documents+scan.Refused, 0, scan.Refused
		report.Declaration = declarationReport(prepared.Declaration)
		report.Warnings = r.warnings(scan, prepared)
		for _, refusal := range scan.Refusals {
			report.Errors = append(report.Errors, ringIngestFailure{Document: refusal.Ordinal, Reason: refusal.text(r.input.name)})
		}
		report.ElapsedMS = time.Since(r.started).Milliseconds()
		writeRingIngestJSON(r.env, report)
	}
}

// ringProgress prints a progress line to stderr: rewritten twice a second on a terminal, one
// line per 100,000 documents otherwise.
type ringProgress struct {
	out        io.Writer
	terminal   bool
	name, into string
	total      int64
	started    time.Time
	lastPrint  time.Time
	lastCount  int64
	wrote      bool
}

func newRingProgress(out io.Writer, terminal bool, name, into string, total int64) *ringProgress {
	now := time.Now()
	return &ringProgress{out: out, terminal: terminal, name: name, into: into, total: total, started: now, lastPrint: now}
}

func (p *ringProgress) update(read int64) {
	now := time.Now()
	if p.terminal {
		if now.Sub(p.lastPrint) < 500*time.Millisecond {
			return
		}
	} else if read-p.lastCount < 100_000 {
		return
	}
	p.lastPrint, p.lastCount = now, read
	text := fmt.Sprintf("ingesting %s → %s: %s of %s documents", p.name, p.into, formatCount(read), formatCount(p.total))
	if p.terminal {
		_, _ = fmt.Fprintf(p.out, "\r%s\x1b[K", text)
		p.wrote = true
		return
	}
	_, _ = fmt.Fprintln(p.out, text)
}

// done ends a rewritten line.
func (p *ringProgress) done() {
	if p.terminal && p.wrote {
		_, _ = fmt.Fprint(p.out, "\r\x1b[K")
	}
}
