package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
)

// restoreVerifyUsage is the one form of the command that works, so a refusal can be
// answered by copying it.
const restoreVerifyUsage = "usage: hoarddb restore <file> --verify"

// restoreVerifyReport is the --json object of restore --verify (section 3.4). The queue
// text is present only when messages are left out, so a reader that tests for the key
// gets the same answer as one that tests the flag.
type restoreVerifyReport struct {
	File                     string `json:"file"`
	Consistency              string `json:"consistency"`
	ConsistencyStatement     string `json:"consistency_statement"`
	QueueMessagesLeftOut     bool   `json:"queue_messages_left_out"`
	QueueMessagesLeftOutText string `json:"queue_messages_left_out_text,omitempty"`
	Databases                int64  `json:"databases"`
	Buckets                  int64  `json:"buckets"`
	Entities                 int64  `json:"entities"`
	Elements                 int64  `json:"elements"`
	Records                  int64  `json:"records"`
}

// runRestoreVerify is `hoarddb restore <file> --verify`: it reads the whole dump once,
// checks it (section 3.1) and prints what it holds. It makes no server contact and reads
// no ring mode, so it works with no server running and cannot send a write. Stdout gets
// the report only when the file verifies; a refusal goes to stderr and exits ExitUsage.
func runRestoreVerify(args []string, stdout, stderr io.Writer) int {
	// Flag errors are reported by this function, with the usage line, so the flag
	// package's own output is discarded (the dump verb does the same).
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	verify := flags.Bool("verify", false, "")
	asJSON := flags.Bool("json", false, "")
	positional, err := parseFlagsAnywhere(flags, args)
	if err != nil {
		return restoreVerifyUsageError(stderr, err.Error())
	}
	if !*verify {
		return restoreVerifyUsageError(stderr, "")
	}
	if len(positional) != 1 {
		return restoreVerifyUsageError(stderr, fmt.Sprintf("takes one dump file, got %d", len(positional)))
	}

	path := positional[0]
	info, err := os.Stat(path)
	if err != nil {
		return restoreVerifyUnreadable(stderr, path, openFailureReason(err))
	}
	// A directory opens and then fails its first read, which the verifier would report as
	// a cut-off file; naming it here tells the operator what they actually passed.
	if info.IsDir() {
		return restoreVerifyUnreadable(stderr, path, errors.New("it is a directory, not a dump file"))
	}

	verification, err := verifyDump(path)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			return restoreVerifyUnreadable(stderr, path, pathErr.Err)
		}
		restoreVerifyStderr(stderr, err.Error()+"\n")
		return ExitUsage
	}

	statement, known := consistencyStatement(verification.Consistency)
	if !known {
		// verifyDump refuses every word that has no statement, so this is a bug here,
		// and the report must not go out without the statement the dump promises.
		restoreVerifyStderr(stderr, fmt.Sprintf("the dump verified with consistency %q, which has no statement; this is a bug in hoarddb\n", verification.Consistency))
		return ExitUsage
	}

	if *asJSON {
		report := restoreVerifyReport{
			File:                 path,
			Consistency:          verification.Consistency,
			ConsistencyStatement: statement,
			QueueMessagesLeftOut: verification.HasQueueField,
			Databases:            verification.Databases,
			Buckets:              verification.Buckets,
			Entities:             verification.Entities,
			Elements:             verification.Elements,
			Records:              verification.Records,
		}
		if verification.HasQueueField {
			report.QueueMessagesLeftOutText = restoreQueueLeftOutLine
		}
		payload, err := json.Marshal(report)
		if err != nil {
			restoreVerifyStderr(stderr, fmt.Sprintf("encoding the report: %v\n", err))
			return ExitUsage
		}
		return writeRestoreVerifyReport(stdout, stderr, string(payload)+"\n")
	}

	lines := []string{statement}
	if verification.HasQueueField {
		lines = append(lines, restoreQueueLeftOutLine)
	}
	lines = append(lines, fmt.Sprintf("Verified %s: databases %s, buckets %s, entities %s, elements %s",
		path, formatCount(verification.Databases), formatCount(verification.Buckets),
		formatCount(verification.Entities), formatCount(verification.Elements)))
	return writeRestoreVerifyReport(stdout, stderr, strings.Join(lines, "\n")+"\n")
}

// writeRestoreVerifyReport writes the whole report in one write. A failed write to stdout
// (a closed pipe) is reported on stderr and exits ExitUsage.
func writeRestoreVerifyReport(stdout, stderr io.Writer, text string) int {
	if _, err := io.WriteString(stdout, text); err != nil {
		restoreVerifyStderr(stderr, fmt.Sprintf("writing the report: %v\n", err))
		return ExitUsage
	}
	return ExitOK
}

// restoreVerifyUsageError prints the usage line, after the reason when there is one, and
// exits ExitUsage. No file was read.
func restoreVerifyUsageError(stderr io.Writer, reason string) int {
	if reason != "" {
		restoreVerifyStderr(stderr, "hoarddb restore: "+reason+"\n")
	}
	restoreVerifyStderr(stderr, restoreVerifyUsage+"\n")
	return ExitUsage
}

// restoreVerifyUnreadable prints the refusal of a path that cannot be read as a dump,
// with the two things an operator can do next, and exits ExitUsage.
func restoreVerifyUnreadable(stderr io.Writer, path string, reason error) int {
	restoreVerifyStderr(stderr, fmt.Sprintf("Cannot read %s: %v. Check the path, or take a new dump with 'hoarddb dump'.\n", path, reason))
	return ExitUsage
}

// openFailureReason returns the operating system's reason for a failed open or stat. The
// path is already in the refusal text, so the *fs.PathError wrapper, which repeats it,
// is removed.
func openFailureReason(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}

// restoreVerifyStderr writes a message to stderr. A failed write has nowhere left to
// report itself: stderr is the last stream there is, so the error is dropped here and
// the exit code still says what happened.
func restoreVerifyStderr(stderr io.Writer, text string) {
	_, _ = io.WriteString(stderr, text)
}
