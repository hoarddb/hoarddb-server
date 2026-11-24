package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringRestoreUsage is the help text of the verb. The not-point-in-time statement and the
// queue sentence are the constants every run prints, so the help cannot drift from what
// the tool does.
func ringRestoreUsage() string {
	return fmt.Sprintf(`Usage: hoarddb restore <file> [flags]

Rebuilds every database, bucket, entity and store element a dump file holds, into a ring
that does not hold them yet. Restore never merges into existing data and never overwrites:
a database that already holds buckets is refused, and the refusal names the command that
restores under another name. Users and roles are not in a dump and are not restored.
Check a file first with 'hoarddb restore --verify <file>'; it needs no server.

%s

%s

Run only one restore per target, and wait for drops to finish before restoring into the
name of a dropped bucket.

Examples:
  hoarddb restore app.hoarddump
  hoarddb restore app.hoarddump --db app --as app_restored
  hoarddb restore --verify app.hoarddump

Flags:
`, restoreConsistencyStatement, restoreQueueLeftOutLine)
}

// ringRestoreEnv is what the verb touches outside its arguments, so a test can run it
// without a process.
type ringRestoreEnv struct {
	stdout, stderr io.Writer
}

// ringRestoreFlags are the parsed flags and the one positional argument.
type ringRestoreFlags struct {
	connection *connectionFlags
	flagSet    *flag.FlagSet
	file       string
	// database restores only that database of the dump; empty restores every one.
	database string
	// as renames the one database --db selects.
	as string
	// members are the --member client addresses of cluster members that publish none.
	members ringMemberAddresses
	jsonOut bool
	verify  bool
}

// restoreUsageError is a flag, an argument or a file problem the restore refused before it
// contacted the server: exit 1, and the text names the fix (AGENTS.md rule 3).
type restoreUsageError struct{ Message string }

func (e *restoreUsageError) Error() string { return terminalSafeText(e.Message) }

// RunRingRestoreCommand implements the standalone 'hoarddb restore' verb on the ring. Every
// message goes to stderr; --json's one object goes to stdout. The exit code is the shared
// table of cli/exitcodes.go.
func RunRingRestoreCommand(args []string) int {
	installClientLogger(false)
	// SIGINT and SIGTERM end the run alike; see verbStopContext.
	ctx, stop := verbStopContext()
	defer stop()
	return runRingRestore(ctx, args, ringRestoreEnv{stdout: os.Stdout, stderr: os.Stderr})
}

// parseRingRestoreArgs reads the flags. An unknown flag, a value that does not parse and a
// wrong number of files are usage errors that end with the line naming the help.
func parseRingRestoreArgs(args []string, env ringRestoreEnv) (*ringRestoreFlags, error) {
	var usage strings.Builder
	flagSet := flag.NewFlagSet("hoarddb restore", flag.ContinueOnError)
	flagSet.SetOutput(&usage)
	parsed := &ringRestoreFlags{connection: registerConnectionFlags(flagSet), flagSet: flagSet}
	flagSet.Lookup("fingerprint").Usage = "pin a server's certificate fingerprint, SHA256:<64 hex>; each server prints its own when it starts. On a cluster restore reads every member, so repeat it once per member (or pass -insecure for nodes you started yourself)"
	flagSet.Var(&parsed.members, "member", ringMemberFlagUsage)
	flagSet.StringVar(&parsed.database, "db", "", "restore only this database of the dump")
	flagSet.StringVar(&parsed.as, "as", "", "restore the database --db selects under this name")
	flagSet.BoolVar(&parsed.jsonOut, "json", false, "print one JSON result object to stdout as well as the text lines on stderr")
	flagSet.BoolVar(&parsed.verify, "verify", false, "only check the file; contact no server")

	positional, err := parseFlagsAnywhere(flagSet, args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(env.stdout, ringRestoreUsage())
			flagSet.SetOutput(env.stdout)
			flagSet.PrintDefaults()
			return nil, flag.ErrHelp
		}
		return parsed, &restoreUsageError{Message: fmt.Sprintf("%s. Run 'hoarddb restore --help' for the flags and examples.", firstLine(usage.String(), err))}
	}
	if len(positional) != 1 {
		return parsed, &restoreUsageError{Message: fmt.Sprintf("restore takes one dump file and got %d. Example: hoarddb restore app.hoarddump. Run 'hoarddb restore --help' for the flags and examples.", len(positional))}
	}
	parsed.file = positional[0]
	return parsed, nil
}

// validate applies the flag rules before anything is read or dialled.
func (f *ringRestoreFlags) validate() error {
	if f.as != "" && f.database == "" {
		return &restoreUsageError{Message: "--as renames one database, so it needs --db to say which. Example: hoarddb restore " + f.file + " --db app --as app_restored. Run 'hoarddb restore --help' for the flags and examples."}
	}
	return nil
}

// checkRestoreSource refuses a source that cannot be read three times: restore verifies the
// whole file, then skims it, then applies it, so a pipe or a terminal would be consumed by
// the first pass. It stats the path, which does not open a pipe and so cannot block on one.
func checkRestoreSource(path string) error {
	if path == "-" {
		return &restoreUsageError{Message: "restore reads the file more than once (it verifies it before it contacts the server), so it cannot read standard input. Save the dump to a file first, then run: hoarddb restore <file>"}
	}
	info, err := os.Stat(path)
	if err != nil {
		return &restoreUsageError{Message: fmt.Sprintf("Cannot read %s: %v. Check the path, or take a new dump with 'hoarddb dump'.", path, openFailureReason(err))}
	}
	if info.IsDir() {
		return &restoreUsageError{Message: fmt.Sprintf("Cannot read %s: it is a directory, not a dump file. Pass the .hoarddump file: hoarddb restore <file>", path)}
	}
	if !info.Mode().IsRegular() {
		return &restoreUsageError{Message: fmt.Sprintf("%s is not a regular file (a pipe or a device), and restore reads the file more than once. Save the dump to a regular file first, then run: hoarddb restore <file>", path)}
	}
	return nil
}

// runRingRestore is the verb: flags first (exit 1, nothing read), then the whole file is
// verified (exit 1, nothing dialled), then the dial (6, 7), then the restore itself.
func runRingRestore(ctx context.Context, args []string, env ringRestoreEnv) int {
	flags, err := parseRingRestoreArgs(args, env)
	if errors.Is(err, flag.ErrHelp) {
		return ExitOK
	}
	if err == nil {
		err = flags.validate()
	}
	if err != nil {
		return refuseRingRestore(env, flags, err)
	}
	if flags.verify {
		return runRestoreVerify(args, env.stdout, env.stderr)
	}
	if err := checkRestoreSource(flags.file); err != nil {
		return refuseRingRestore(env, flags, err)
	}

	sourceInfo, err := os.Stat(flags.file)
	if err != nil {
		return refuseRingRestore(env, flags, err)
	}
	verification, err := verifyDump(flags.file)
	if err != nil {
		return refuseRingRestore(env, flags, err)
	}
	statement, known := consistencyStatement(verification.Consistency)
	if !known {
		return refuseRingRestore(env, flags, fmt.Errorf("the dump verified with consistency %q, which has no statement; this is a bug in hoarddb", verification.Consistency))
	}
	// The statement goes before any other line, so an operator sees what the dump promises
	// even when the rest of the output scrolls away (section 3.4, pass 1).
	_, _ = fmt.Fprintln(env.stderr, statement)
	if verification.HasQueueField {
		_, _ = fmt.Fprintln(env.stderr, restoreQueueLeftOutLine)
	}

	client, _, options, redial, exit := dialRingForMembers(ctx, env.stderr, flags.flagSet, flags.connection)
	if client == nil {
		return exit
	}
	defer func() { _ = client.Close() }()
	run := &ringRestoreRun{
		ctx:          ctx,
		stderr:       inertWriter{env.stderr},
		stdout:       env.stdout,
		flags:        flags,
		verification: verification,
		client:       client,
		redial:       redial,
		sourceInfo:   sourceInfo,
		started:      time.Now(),
	}
	// The checks that the target is empty list buckets and documents, which a cluster member
	// answers only for what it owns, so they read every member (BUG-0903). Writes still go
	// through the one connection: a member forwards a write to its owner.
	members, err := openRingMembers(ctx, ringMemberDial{entry: client, options: options, addresses: flags.members, notice: env.stderr, verb: "restore"}, restoreScope(flags))
	if err != nil {
		return run.refuse(withNothingWritten(err))
	}
	defer members.close()
	run.members, run.target = members, members
	return run.execute()
}

// restoreScope is the target databases a restore can touch, for the member set's check that each
// has an owner for every slot: the --db one under its --as name, or every database (nil).
func restoreScope(flags *ringRestoreFlags) []string {
	if flags.database == "" {
		return nil
	}
	return []string{flags.restoreTargetName(flags.database)}
}

// withNothingWritten adds restore's sentence to a refusal that came before any write.
func withNothingWritten(err error) error {
	failed := ringFailure(err)
	return &RingError{Code: failed.Code, Exit: failed.Exit, Err: err, Message: strings.TrimSuffix(failed.Message, ".") + ". Nothing was written."}
}

// refuseRingRestore prints a refusal that ended the run before the server was contacted
// and returns exit 1: a flag, an unreadable or damaged file. A damaged file's text is the
// one section 3.1 fixes, word for word, so it is printed without a prefix.
func refuseRingRestore(env ringRestoreEnv, flags *ringRestoreFlags, err error) int {
	_, _ = fmt.Fprintln(env.stderr, terminalSafeText(err.Error()))
	if flags != nil && flags.jsonOut {
		writeRestoreJSON(env.stdout, env.stderr, ringRestoreJSONFailure{Error: terminalSafeText(err.Error()), Exit: ExitUsage})
	}
	return ExitUsage
}

// ringRestoreJSONFailure is the one object --json prints for a run that did not finish.
type ringRestoreJSONFailure struct {
	Error string `json:"error"`
	Exit  int    `json:"exit"`
	// State is the report of what the run left behind (section 3.5), empty when nothing
	// was created.
	State string `json:"state,omitempty"`
}

// writeRestoreJSON prints one object on stdout. A stdout that cannot take it is reported on
// stderr and does not change the run's exit: the restore is what the run is for.
func writeRestoreJSON(stdout, stderr io.Writer, object any) {
	encoded, err := json.Marshal(object)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "restore: cannot encode the --json result: %v\n", err)
		return
	}
	_, _ = fmt.Fprintln(stdout, string(encoded))
}

// ringRestoreRun is one restore after the dial: the connection, the flags and the file
// that pass 1 verified.
type ringRestoreRun struct {
	ctx          context.Context
	stderr       io.Writer
	stdout       io.Writer
	flags        *ringRestoreFlags
	verification dumpVerification
	client       *ringclient.Client
	redial       ringRedial
	// target answers the pass-2 reads; nil means the client, and a test replaces it.
	target restoreTargetSource
	// creator answers the creates of pass 3; nil means the client, and a test replaces it.
	creator restoreCreateSource
	// members is the cluster's member set, nil when a test drives the run without one: the
	// emptiness checks read every member.
	members *ringMemberSet
	// links is pass 2's order for the buckets that take part in a link.
	links *restoreLinkPlan
	// sourceInfo is the dump's size and time as pass 1 saw them.
	sourceInfo os.FileInfo
	started    time.Time
}

// restoreAfterPassTwo is a seam for tests, nil in the binary: it runs between pass 2 and the
// first write, which is the window two restores of one dump, or an application, can race in,
// and a test cannot reach that window by timing.
var restoreAfterPassTwo func()

// execute is the run after the dial: pass 2 reads the target and refuses what cannot be
// restored, then pass 3 writes the file. A restore that stops says what it left behind.
func (r *ringRestoreRun) execute() int {
	if err := r.checkTargets(); err != nil {
		return r.refuse(err)
	}
	if err := r.checkSourceUnchanged(); err != nil {
		return r.refuse(err)
	}
	if restoreAfterPassTwo != nil {
		restoreAfterPassTwo()
	}
	_, _ = fmt.Fprintf(r.stderr, "Restoring %s from %s: databases %s, buckets %s, entities %s, elements %s\n",
		r.scopeText(), r.flags.file, formatCount(r.verification.Databases), formatCount(r.verification.Buckets),
		formatCount(r.verification.Entities), formatCount(r.verification.Elements))
	sender := newRestoreSender(r.client, r.redial)
	defer sender.close()
	applier := newRestoreApplier(r, sender)
	if err := applier.apply(); err != nil {
		return r.fail(err, applier)
	}
	return r.finish(applier)
}

// scopeText says what part of the file this run restores.
func (r *ringRestoreRun) scopeText() string {
	if r.flags.database == "" {
		return "every database"
	}
	return "database " + terminalSafeText(r.flags.database)
}

// checkOpenedSource refuses a file that is not the one pass 1 verified: the open file must be
// the same file (not another renamed over the path) with the same size and time, so a swap
// after the first check is caught before the first write. The checksum compare at the end of
// pass 3 closes the case of a same-size, same-time rewrite.
func (r *ringRestoreRun) checkOpenedSource(file *os.File) error {
	if r.sourceInfo == nil {
		return nil
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("reading the dump %s again: %w", r.flags.file, err)
	}
	if !os.SameFile(info, r.sourceInfo) || info.Size() != r.sourceInfo.Size() || !info.ModTime().Equal(r.sourceInfo.ModTime()) {
		return &RingError{Exit: ExitUsage, Message: fmt.Sprintf("The dump %s changed after it was verified. Nothing was written. Run the restore again.", r.flags.file)}
	}
	return nil
}

// checkSourceUnchanged refuses a file whose size or modification time moved between pass 1
// and pass 3: pass 3 decodes every record strictly and checks the checksum at the end, but
// a file that changed would reach the server before that end, so a change that is visible
// before the first write stops the run here, with nothing created.
func (r *ringRestoreRun) checkSourceUnchanged() error {
	info, err := os.Stat(r.flags.file)
	if err != nil {
		return fmt.Errorf("reading the dump %s again: %w", r.flags.file, err)
	}
	if r.sourceInfo != nil && (info.Size() != r.sourceInfo.Size() || !info.ModTime().Equal(r.sourceInfo.ModTime())) {
		return &RingError{Exit: ExitUsage, Message: fmt.Sprintf("The dump %s changed after it was verified. Nothing was written. Run the restore again.", r.flags.file)}
	}
	return nil
}

// refuse prints a refusal that came before anything was written and returns its exit: the
// text is the refusal's own, which names the fix.
func (r *ringRestoreRun) refuse(err error) int {
	failed := ringFailure(err)
	_, _ = fmt.Fprintln(r.stderr, terminalSafeText(failed.Message))
	if r.flags.jsonOut {
		writeRestoreJSON(r.stdout, r.stderr, ringRestoreJSONFailure{Error: terminalSafeText(failed.Message), Exit: failed.Exit})
	}
	return failed.Exit
}

// fail ends a restore that stopped after pass 2: it names the reason and exits with the
// failure's class. An interrupt is exit 4 whatever call it landed in.
func (r *ringRestoreRun) fail(err error, applier *restoreApplier) int {
	exit := restoreExit(r.ctx, err)
	reason := "restore stopped: " + terminalSafeText(strings.TrimPrefix(err.Error(), "ringclient: "))
	if exit == ExitInterrupted {
		reason = "restore interrupted"
	}
	_, _ = fmt.Fprintln(r.stderr, reason)
	state := r.stateReport(applier)
	_, _ = fmt.Fprintln(r.stderr, state)
	if r.flags.jsonOut {
		writeRestoreJSON(r.stdout, r.stderr, ringRestoreJSONFailure{Error: reason, Exit: exit, State: state})
	}
	return exit
}

// restoreExit is the exit code of a failure: 4 for an interrupt, 1 for a dump that does not
// hold together, 8 for a frame that was sent and never answered, and the refusal's own class
// for everything the ring answered.
func restoreExit(ctx context.Context, err error) int {
	var damage *restoreDamageError
	var unknown *restoreUnknownError
	var outOfOrder *restoreOutOfOrderError
	switch {
	case ctx.Err() != nil:
		return ExitInterrupted
	case errors.As(err, &damage):
		return ExitUsage
	case errors.As(err, &unknown), errors.As(err, &outOfOrder):
		return ExitUnavailable
	}
	return ringFailure(err).Exit
}

// finish prints the summary of a restore that completed and returns exit 0.
func (r *ringRestoreRun) finish(applier *restoreApplier) int {
	tally := applier.tally
	_, _ = fmt.Fprintf(r.stderr, "Restored %s: databases %s, buckets %s, entities %s, elements %s in %s.\n",
		r.flags.file, formatCount(tally.databases), formatCount(tally.buckets), formatCount(tally.entities), formatCount(tally.elements),
		time.Since(r.started).Round(time.Millisecond))
	if tally.blobFields > 0 {
		_, _ = fmt.Fprintf(r.stderr, "%s blob fields restored empty: payloads are not in a dump.\n", formatCount(tally.blobFields))
	}
	if tally.aggregates > 0 {
		_, _ = fmt.Fprintf(r.stderr, "%s aggregates are rebuilding on the server and report building until they catch up.\n", formatCount(tally.aggregates))
	}
	if r.verification.HasQueueField {
		_, _ = fmt.Fprintln(r.stderr, restoreQueueLeftOutLine)
	}
	if r.flags.jsonOut {
		object := ringRestoreJSON{
			File: r.flags.file, Databases: tally.databases, Buckets: tally.buckets, Entities: tally.entities,
			Elements: tally.elements, BlobFieldsLeftEmpty: tally.blobFields, AggregatesRebuilding: tally.aggregates,
			Consistency: ringDumpJSONConsistency{Word: r.verification.Consistency, Statement: restoreConsistencyStatement},
		}
		if r.verification.HasQueueField {
			object.QueueMessagesLeftOut, object.QueueMessagesLeftOutText = true, restoreQueueLeftOutLine
		}
		writeRestoreJSON(r.stdout, r.stderr, object)
	}
	return ExitOK
}

// ringRestoreJSON is the one object --json prints after the text lines of a restore that
// completed.
type ringRestoreJSON struct {
	File                     string                  `json:"file"`
	Databases                int64                   `json:"databases"`
	Buckets                  int64                   `json:"buckets"`
	Entities                 int64                   `json:"entities"`
	Elements                 int64                   `json:"elements"`
	BlobFieldsLeftEmpty      int64                   `json:"blob_fields_left_empty"`
	AggregatesRebuilding     int64                   `json:"aggregates_rebuilding"`
	Consistency              ringDumpJSONConsistency `json:"consistency"`
	QueueMessagesLeftOut     bool                    `json:"queue_messages_left_out,omitempty"`
	QueueMessagesLeftOutText string                  `json:"queue_messages_left_out_text,omitempty"`
}
