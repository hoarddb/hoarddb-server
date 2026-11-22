package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/version"
)

// ringDumpNoDatabasesText is the refusal of a ring that holds no database the session may
// see (section 3.2 item 1): an empty file would restore into nothing and look like a
// backup, so the run stops before the writer opens.
const ringDumpNoDatabasesText = `The ring on this server holds no databases that user %q may see. Check the user's permissions, or create one with 'create database <name>'.`

// ringDumpDatabasePageSize is how many databases one ShowDatabases page asks for. The dump
// compares the page against the names it was asked for and keeps no other name, so the page
// size is the only thing this figure trades: fewer round trips for more memory in one page.
const ringDumpDatabasePageSize = 500

// dumpToolName is the header's tool member: this build's release word and nothing else.
// version.String() is the machine-readable "version=... commit=... build_date=..." line -v
// prints, which is not what a reader of a dump wants to see beside the file's format.
func dumpToolName() string {
	return "hoarddb " + version.Version
}

// ringDefaultDumpName is the --out default: one file in the current directory, named for
// the moment the run started so two dumps do not collide.
func ringDefaultDumpName(now time.Time) string {
	return fmt.Sprintf("hoarddb-%s.hoarddump", now.UTC().Format("20060102T150405Z"))
}

// ringDumpUsage is the help text of the verb. The not-point-in-time statement and the
// queue sentence are the constants every run prints, so the help cannot drift from what
// the tool does (section 3.1, "What the tool prints", rev 7.1).
func ringDumpUsage() string {
	return fmt.Sprintf(`Usage: hoarddb dump [flags]

Writes one file that holds every database, bucket, entity and store element this session
may see. It writes a new file and refuses an existing one, so a dump never overwrites a backup.
Check a file with 'hoarddb restore --verify <file>'; it needs no server.
Restore a dump into an empty ring with 'hoarddb restore <file>'.

%s

%s

For a copy of one moment: stop the writers (or dump during a quiet period), then run the
same command again.

Examples:
  hoarddb dump
  hoarddb dump --out app.hoarddump
  hoarddb dump --db app --out app.hoarddump
  hoarddb dump --db app --bucket users --out users.hoarddump
  hoarddb dump --db app --db audit --out two-databases.hoarddump
  hoarddb dump --db app --parallel 4 --out app-four-at-once.hoarddump

Flags:
`, dumpStartStatement, dumpQueueLeftOutLine)
}

// ringDumpEnv is what the verb touches outside its arguments, so a test can run it without
// a process.
type ringDumpEnv struct {
	stdout, stderr io.Writer
}

// ringDumpDatabases is the repeatable --db flag: every name given, in the order given.
type ringDumpDatabases []string

func (d *ringDumpDatabases) String() string { return strings.Join(*d, ",") }

// Set adds one database name, so --db may be repeated.
func (d *ringDumpDatabases) Set(value string) error {
	*d = append(*d, value)
	return nil
}

// ringDumpFlags are the parsed flags. There is no positional argument: the scope is named
// with --db and --bucket.
type ringDumpFlags struct {
	connection *connectionFlags
	flagSet    *flag.FlagSet
	out        string
	databases  ringDumpDatabases
	bucket     string
	// jsonOut writes the one --json object to stdout: the summary of a run that wrote a
	// file, or the failure object of one that did not (section 3.1).
	jsonOut bool
	// parallel is how many entities of one List page are read at once (section 3.3). 1
	// reads them one at a time, straight into the file, with no buffer.
	parallel int
}

// dumpUsageError is a flag, an argument or a file problem the dump refused before it wrote
// anything: exit 1, and the text names the fix (AGENTS.md rule 3).
type dumpUsageError struct{ Message string }

func (e *dumpUsageError) Error() string { return terminalSafeText(e.Message) }

// RunRingDumpCommand implements the standalone 'hoarddb dump' verb on the ring. Progress
// and every message go to stderr; --json's one object goes to stdout, so a script can read
// the result without parsing the report. The exit code is the shared table of
// cli/exitcodes.go.
func RunRingDumpCommand(args []string) int {
	installClientLogger(false)
	// SIGINT and SIGTERM end the run alike; see verbStopContext.
	ctx, stop := verbStopContext()
	defer stop()
	return runRingDump(ctx, args, ringDumpEnv{stdout: os.Stdout, stderr: os.Stderr})
}

// ringDumpJSON is the one object --json prints after the text lines of a run that wrote
// its file (section 3.1, "What the tool prints").
type ringDumpJSON struct {
	File        string                  `json:"file"`
	Databases   int64                   `json:"databases"`
	Buckets     int64                   `json:"buckets"`
	Entities    int64                   `json:"entities"`
	Elements    int64                   `json:"elements"`
	Consistency ringDumpJSONConsistency `json:"consistency"`
	// QueueMessagesLeftOut is true when the dump held a fifoStore or lifoStore field: the
	// claimed and dead-lettered messages of such a field are in no list reply, so they are
	// not in the file (section 3.1, rev 7). The text is the sentence the run printed.
	QueueMessagesLeftOut     bool   `json:"queue_messages_left_out,omitempty"`
	QueueMessagesLeftOutText string `json:"queue_messages_left_out_text,omitempty"`
}

// ringDumpJSONConsistency is the header's consistency word and the statement it maps to,
// so a script can say what the file is without reading the header record.
type ringDumpJSONConsistency struct {
	Word      string `json:"word"`
	Statement string `json:"statement"`
}

// ringDumpJSONFailure is the one object --json prints for a run that wrote no file: a
// script reading stdout always gets one object, and exit says which kind of failure it
// was. File is always empty, because the final name never exists on a failed run.
type ringDumpJSONFailure struct {
	File  string `json:"file"`
	Error string `json:"error"`
	Exit  int    `json:"exit"`
}

// writeRingDumpJSON prints one object on stdout. A stdout that cannot take it is reported
// on stderr and does not change the run's exit: the file is what the run is for.
func writeRingDumpJSON(env ringDumpEnv, object any) {
	encoded, err := json.Marshal(object)
	if err != nil {
		_, _ = fmt.Fprintf(env.stderr, "dump: cannot encode the --json result: %v\n", err)
		return
	}
	_, _ = fmt.Fprintln(env.stdout, string(encoded))
}

// runRingDump is the verb: flags first (exit 1, nothing dialled), then the dial (6, 7),
// then what the session may see (2), then the file.
func runRingDump(ctx context.Context, args []string, env ringDumpEnv) int {
	return runRingDumpWithSource(ctx, args, env, nil, "")
}

// runRingDumpWithSource is runRingDump with the ring replaced by source and the session
// named by user, which a test supplies so it can script the answers and count the calls.
// A nil source dials the ring itself and takes the user from the dial. The flags, the
// checks and the file are the same on both paths.
func runRingDumpWithSource(ctx context.Context, args []string, env ringDumpEnv, source ringDumpSource, user string) int {
	flags, err := parseRingDumpArgs(args, env)
	if errors.Is(err, flag.ErrHelp) {
		return ExitOK
	}
	if err != nil {
		return refuseRingDump(env, flags, err)
	}
	if err := flags.validate(); err != nil {
		return refuseRingDump(env, flags, err)
	}
	if source == nil {
		client, dialedUser, _, exit := dialRingForVerbUser(ctx, env.stderr, flags.flagSet, flags.connection)
		if client == nil {
			return exit
		}
		defer func() { _ = client.Close() }()
		source, user = ringClientSource{client: client}, dialedUser
	}
	run := &ringDumpRun{
		ctx:      ctx,
		source:   source,
		stderr:   inertWriter{env.stderr},
		flags:    flags,
		user:     user,
		parallel: flags.parallel,
	}
	return run.execute(env)
}

// refuseRingDump prints a usage refusal and returns exit 1: a flag, an argument or a file
// problem that ended the run before a file was written. It keeps its own text, because
// running the same command again cannot fix a flag or an existing --out. Every failure
// after the dial goes through ringDumpRun.fail instead (section 3.5).
func refuseRingDump(env ringDumpEnv, flags *ringDumpFlags, err error) int {
	_, _ = fmt.Fprintf(env.stderr, "dump: %s\n", terminalSafeText(err.Error()))
	if flags != nil && flags.jsonOut {
		writeRingDumpJSON(env, ringDumpJSONFailure{Error: terminalSafeText(err.Error()), Exit: ExitUsage})
	}
	return ExitUsage
}

// parseRingDumpArgs reads the flags. An unknown flag, a value that does not parse and a
// positional argument are all usage errors that end with the line naming the help.
func parseRingDumpArgs(args []string, env ringDumpEnv) (*ringDumpFlags, error) {
	var usage strings.Builder
	fs := flag.NewFlagSet("hoarddb dump", flag.ContinueOnError)
	fs.SetOutput(&usage)
	parsed := &ringDumpFlags{connection: registerConnectionFlags(fs), flagSet: fs}
	// The ring pins a fingerprint (ringdial.go), which the legacy flag text still calls
	// unsupported.
	fs.Lookup("fingerprint").Usage = "pin the server's certificate fingerprint, SHA256:<64 hex>; the server prints it when it starts (or pass -insecure for a node you started yourself)"
	fs.StringVar(&parsed.out, "out", ringDefaultDumpName(time.Now()), "the file to write; a plain name goes in the current directory")
	fs.Var(&parsed.databases, "db", "dump only this database; repeat for more than one")
	fs.StringVar(&parsed.bucket, "bucket", "", "dump only this bucket; needs exactly one --db")
	// The --json object is written once, after the run: the summary of a file that was
	// completed, or the failure object of one that was not (section 3.1).
	fs.BoolVar(&parsed.jsonOut, "json", false, "print one JSON result object to stdout as well as the text summary on stderr")
	fs.IntVar(&parsed.parallel, "parallel", ringDumpDefaultParallel, "how many entities of a bucket are read at once, from 1 to 64; for a database nobody writes to, the file is the same whatever the number. One reader holds up to about 170 MiB with maximum-size elements (measured), and the total is not linear in the number (8 readers measured about 290 MiB); a worst case at 64 was not measured")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(env.stdout, ringDumpUsage())
			fs.SetOutput(env.stdout)
			fs.PrintDefaults()
			return nil, flag.ErrHelp
		}
		return parsed, &dumpUsageError{Message: fmt.Sprintf("%s. Run 'hoarddb dump --help' for the flags and examples.", firstLine(usage.String(), err))}
	}
	if positionals := fs.Args(); len(positionals) > 0 {
		return parsed, &dumpUsageError{Message: fmt.Sprintf("dump takes no positional arguments, and got %d (%s). Name the scope with --db and --bucket and the file with --out: hoarddb dump --db app --out app.hoarddump. Run 'hoarddb dump --help' for the flags and examples.",
			len(positionals), strings.Join(positionals, " "))}
	}
	return parsed, nil
}

// validate applies the flag rules before anything is dialled: a bucket is scoped by exactly
// one database (section 3.1).
func (f *ringDumpFlags) validate() error {
	if f.out == "" {
		return &dumpUsageError{Message: "dump has no --out, so it has no file to write. Example: hoarddb dump --out app.hoarddump. Run 'hoarddb dump --help' for the flags and examples."}
	}
	if f.bucket != "" && len(f.databases) != 1 {
		return &dumpUsageError{Message: "--bucket needs exactly one --db. Example: hoarddb dump --db app --bucket users. Run 'hoarddb dump --help' for the flags and examples."}
	}
	if f.parallel < 1 || f.parallel > ringDumpMaxParallel {
		return &dumpUsageError{Message: fmt.Sprintf("--parallel must be a number from 1 to %d, and got %d. Example: hoarddb dump --parallel %d. Run 'hoarddb dump --help' for the flags and examples.",
			ringDumpMaxParallel, f.parallel, ringDumpDefaultParallel)}
	}
	return nil
}

// ringDumpRun is one dump: the calls it may make (source), the file it writes and what it
// has counted so far. Later steps add their enumerators to the source and their writes to
// this struct; nothing here keeps a list of database, bucket or entity names.
type ringDumpRun struct {
	ctx      context.Context
	source   ringDumpSource
	writer   *dumpWriter
	counters dumpCounters
	stderr   io.Writer
	flags    *ringDumpFlags
	// user is the name the session authenticated as, from the dial: the header records it
	// and the final report says the file holds that user's view.
	user string
	// heldQueueField is true once the run has written a bucket that declares a fifoStore or
	// lifoStore field. Claimed and dead-lettered messages of such a field are in no list
	// reply, so the closing lines say they are not in the file (section 3.1).
	heldQueueField bool
	// parallel is how many entities of one List page are read at once, from the flag (3.3).
	parallel int
	// spiller makes and counts the spill files of the entity buffers, one per run. It is
	// created with the writer, before the first buffer, and it holds no file until an
	// entity is large enough to need one.
	spiller *dumpSpiller
}

// execute is the run after the dial: what the session may see, then the file. Every
// failure ends in fail, which says no file was written and how to recover (section 3.5).
func (r *ringDumpRun) execute(env ringDumpEnv) int {
	if err := r.checkDatabases(); err != nil {
		return r.fail(env, err)
	}
	if err := r.writeFile(); err != nil {
		return r.fail(env, err)
	}
	r.printJSON(env)
	return ExitOK
}

// fail is the one failure path after the dial. An interrupted run says so and exits 4; a
// usage refusal keeps its own text and exit 1, because running the command again cannot fix
// it; every other failure names its reason, says the file was not written, tells the
// operator to run the same command again, and exits with the failure's own class. Whatever
// the error was - a ring refusal, a lost connection, a full disk - the operator reads the
// same closing sentence, so nobody has to guess whether a file was left behind.
func (r *ringDumpRun) fail(env ringDumpEnv, err error) int {
	if r.ctx.Err() != nil {
		const interrupted = "interrupted"
		_, _ = fmt.Fprintf(r.stderr, "dump: %s. No dump file was written. Run the same command again.\n", interrupted)
		if r.flags.jsonOut {
			writeRingDumpJSON(env, ringDumpJSONFailure{Error: interrupted, Exit: ExitInterrupted})
		}
		return ExitInterrupted
	}
	var usage *dumpUsageError
	if errors.As(err, &usage) {
		return refuseRingDump(env, r.flags, err)
	}
	failed := ringFailure(err)
	// One trailing period is the format's: the sentence below adds it back, so a message
	// that already ends in one does not print two.
	reason := terminalSafeText(strings.TrimSuffix(failed.Message, "."))
	_, _ = fmt.Fprintf(r.stderr, "dump: %s. %s\n", reason, closingSentence(failed.Exit))
	if r.flags.jsonOut {
		writeRingDumpJSON(env, ringDumpJSONFailure{Error: reason, Exit: failed.Exit})
	}
	return failed.Exit
}

// closingSentence is what a failed run says after its reason, by the failure's class. A
// refusal (exit 2) repeats identically, so it gives no advice to run again: its reason
// already names the fix. Any other class (a lost connection, an unknown outcome) is a
// state that can pass, so the sentence says that no complete file exists, which is always
// true because the final name is written only after the footer, and that a new run is safe.
func closingSentence(exit int) string {
	if exit == ExitRefused {
		return "No dump file was written."
	}
	return "No complete dump file was written. You can start the dump again."
}

// printJSON writes the one --json object of a completed run, after the text lines, so a
// script gets the path, the counters and the consistency word in one read (section 3.1).
func (r *ringDumpRun) printJSON(env ringDumpEnv) {
	if !r.flags.jsonOut {
		return
	}
	object := ringDumpJSON{
		File:      r.writer.finalPath,
		Databases: r.counters.Databases,
		Buckets:   r.counters.Buckets,
		Entities:  r.counters.Entities,
		Elements:  r.counters.Elements,
		Consistency: ringDumpJSONConsistency{
			Word:      dumpConsistencyWord,
			Statement: dumpStartStatement,
		},
	}
	if r.heldQueueField {
		object.QueueMessagesLeftOut = true
		object.QueueMessagesLeftOutText = dumpQueueLeftOutLine
	}
	writeRingDumpJSON(env, object)
}

// checkDatabases pages the databases the session may see and refuses the run before the
// writer opens: a --db the session cannot see is a refusal that names what exists, and a
// ring with no database at all is an error rather than an empty file (section 3.2 item 1).
// It compares each listed name against the requested ones and keeps nothing else, so its
// memory is the flag list and never the ring's size.
func (r *ringDumpRun) checkDatabases() error {
	wanted := make(map[string]bool, len(r.flags.databases))
	for _, name := range r.flags.databases {
		wanted[name] = true
	}
	seen := make(map[string]bool, len(wanted))
	sawAny := false
	var cursor []byte
	for {
		page, err := r.source.ShowDatabases(r.ctx, ringclient.PageRequest{Limit: ringDumpDatabasePageSize, Cursor: cursor})
		if err != nil {
			return fmt.Errorf("listing the databases this session may see: %w", err)
		}
		for _, database := range page.Databases {
			sawAny = true
			if wanted[database.Name] {
				seen[database.Name] = true
			}
		}
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	if len(wanted) == 0 && !sawAny {
		return &RingError{Message: fmt.Sprintf(ringDumpNoDatabasesText, r.user), Exit: ExitRefused}
	}
	missing := make([]string, 0, len(r.flags.databases))
	for _, name := range r.flags.databases {
		if !seen[name] {
			missing = append(missing, fmt.Sprintf("Database %q not found. Run 'show databases' to list what exists.", name))
		}
	}
	if len(missing) > 0 {
		return &RingError{Message: strings.Join(missing, "\n"), Exit: ExitRefused}
	}
	return nil
}

// writeFile opens the file, writes the header, enumerates the databases and buckets
// and completes the file. Any failure after the writer opened aborts it, so a cut-off
// file never lies under the name an operator would restore from (section 3.5).
func (r *ringDumpRun) writeFile() error {
	directory := filepath.Dir(r.flags.out)
	if info, statErr := os.Stat(directory); errors.Is(statErr, fs.ErrNotExist) {
		return &dumpUsageError{Message: fmt.Sprintf("The directory %s does not exist. Create it (mkdir -p %s) or pick another --out.", directory, directory)}
	} else if statErr == nil && !info.IsDir() {
		return &dumpUsageError{Message: fmt.Sprintf("%s is a file, not a directory, so --out cannot go inside it. Pick another --out.", directory)}
	}
	writer, err := openDumpWriter(directory, filepath.Base(r.flags.out))
	if err != nil {
		return &dumpUsageError{Message: err.Error()}
	}
	r.writer = writer
	// One spiller per run, beside the file it feeds (section 3.3): a spill file is then in
	// the directory an operator already cleans up in, and the count never grows with the
	// ring.
	r.spiller = newDumpSpiller(directory, filepath.Base(r.flags.out))
	if dumpSpillerCreated != nil {
		dumpSpillerCreated(r.spiller)
	}
	// The statement goes before the first progress line, and the end line repeats it after
	// the last one (section 3.1, "What the tool prints").
	_, _ = fmt.Fprintln(r.stderr, dumpStartStatement)
	if err := r.writeHeader(); err != nil {
		return r.abandon(err)
	}
	// The databases and buckets go between the header and the summary; the documents and
	// store elements follow in the later steps of the slice (Da3 and Da4).
	if err := r.enumerate(); err != nil {
		return r.abandon(err)
	}
	if err := r.writer.Close(r.counters); err != nil {
		return r.abandon(err)
	}
	r.printEnd()
	return nil
}

// abandon aborts a file that is not complete and returns the error it was given, so the
// caller's exit is the failure's own and not the abort's.
func (r *ringDumpRun) abandon(err error) error {
	if abortErr := r.writer.Abort(); abortErr != nil {
		return errors.Join(err, abortErr)
	}
	return err
}

// writeHeader writes the first record: what wrote the file, which user's view it holds,
// which scope was asked for and the one consistency promise of format version 1.
func (r *ringDumpRun) writeHeader() error {
	header := &dumpHeader{
		T:           dumpKindHeader,
		V:           dumpFormatVersion,
		Created:     time.Now().UTC(),
		Tool:        dumpToolName(),
		User:        r.user,
		Scope:       ringDumpScope(r.flags.databases, r.flags.bucket),
		Consistency: dumpConsistencyWord,
	}
	if err := r.writer.WriteRecord(header); err != nil {
		return fmt.Errorf("writing the dump header: %w", err)
	}
	return nil
}

// ringDumpScope is the header's scope member: all, db:<name>, db:a,db:b for more than one
// database, or db:<name>/bucket:<name> (section 3.1).
func ringDumpScope(databases []string, bucket string) string {
	if len(databases) == 0 {
		return "all"
	}
	if len(databases) == 1 && bucket != "" {
		return "db:" + databases[0] + "/bucket:" + bucket
	}
	return "db:" + strings.Join(databases, ",db:")
}

// printEnd writes the closing lines: what the file does not hold (the databases the user
// may not see), and the end
// line that repeats the not-point-in-time statement with the counters.
func (r *ringDumpRun) printEnd() {
	_, _ = fmt.Fprintf(r.stderr, "Dumped what user %q may see. Databases that user may not see are not in this file.\n", r.user)
	_, _ = fmt.Fprintf(r.stderr, dumpEndStatementFormat+"\n", r.writer.finalPath, r.counters.Entities, r.counters.Elements)
	// The queue sentence follows the end line, and only for a dump that holds a queue field:
	// a run of scalars and keyed stores leaves nothing queued out (section 3.1, rev 7).
	if r.heldQueueField {
		_, _ = fmt.Fprintln(r.stderr, dumpQueueLeftOutLine)
	}
}
