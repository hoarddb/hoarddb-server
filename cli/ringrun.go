package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/termio"
	"github.com/hoarddb/hoarddb-server/version"
)

// ringNameTTL is how long completion trusts the names it fetched: long enough
// that holding Tab does not send a request per key, short enough that a bucket
// created a moment ago shows up.
const ringNameTTL = 5 * time.Second

// ringNameCall bounds one completion lookup so a slow server cannot freeze the prompt.
const ringNameCall = 2 * time.Second

// ringServerNames is the NameSource of Tab completion. It asks the server through
// the session's RingAPI and caches each answer for ringNameTTL, databases included.
type ringServerNames struct {
	session *RingSession
	now     func() time.Time
	lock    sync.Mutex
	cached  map[string]ringNameEntry
}

type ringNameEntry struct {
	at    time.Time
	names []string
}

func newRingServerNames(session *RingSession) *ringServerNames {
	return &ringServerNames{session: session, now: time.Now, cached: map[string]ringNameEntry{}}
}

// fetch returns the cached names under key, or asks load once the entry is old.
// A failed lookup offers no names: completion never shows an error.
func (n *ringServerNames) fetch(key string, load func(ctx context.Context) ([]string, error)) []string {
	n.lock.Lock()
	defer n.lock.Unlock()
	if entry, ok := n.cached[key]; ok && n.now().Sub(entry.at) < ringNameTTL {
		return entry.names
	}
	ctx, cancel := context.WithTimeout(context.Background(), ringNameCall)
	defer cancel()
	names, err := load(ctx)
	if err != nil {
		return nil
	}
	n.cached[key] = ringNameEntry{at: n.now(), names: names}
	return names
}

// Databases lists the databases the session may see, following the cursor to the end.
func (n *ringServerNames) Databases() []string {
	return n.fetch("databases", func(ctx context.Context) ([]string, error) {
		var names []string
		var cursor []byte
		for {
			page, err := n.session.api.ShowDatabases(ctx, ringPage(cursor))
			if err != nil {
				return nil, err
			}
			for _, database := range page.Databases {
				names = append(names, database.Name)
			}
			if !page.More || len(names) > 10000 {
				return names, nil
			}
			cursor = page.Cursor
		}
	})
}

// Buckets lists a database's buckets, following the cursor to the end.
func (n *ringServerNames) Buckets(database string) []string {
	return n.fetch("buckets/"+database, func(ctx context.Context) ([]string, error) {
		var names []string
		var cursor []byte
		for {
			page, err := n.session.api.ShowBuckets(ctx, database, ringPage(cursor))
			if err != nil {
				return nil, err
			}
			for _, bucket := range page.Buckets {
				names = append(names, bucket.Name)
			}
			if !page.More || len(names) > 10000 {
				return names, nil
			}
			cursor = page.Cursor
		}
	})
}

// Fields lists every declared field of a bucket.
func (n *ringServerNames) Fields(database, bucket string) []string {
	return n.declared(database, bucket, false)
}

// StoreFields lists the declared store fields of a bucket.
func (n *ringServerNames) StoreFields(database, bucket string) []string {
	return n.declared(database, bucket, true)
}

func (n *ringServerNames) declared(database, bucket string, storesOnly bool) []string {
	key := fmt.Sprintf("fields/%v/%s/%s", storesOnly, database, bucket)
	return n.fetch(key, func(ctx context.Context) ([]string, error) {
		info, err := n.session.api.DescribeBucket(ctx, database, bucket)
		if err != nil {
			return nil, err
		}
		var names []string
		for _, field := range info.Fields {
			if field.Name != "_id_" && (!storesOnly || ringStoreType(field.Type)) {
				names = append(names, field.Name)
			}
		}
		return names, nil
	})
}

// Nodes lists no node: the ring wire has no request for the members.
func (n *ringServerNames) Nodes() []string { return nil }

// Prompt is the prompt of v1.0: the theme's, with the selected database. The mode is in the banner.
func (s *RingSession) Prompt() string { return s.theme.renderPrompt(s.selected) }

// Completer returns the Tab completer of the session, fed by the grammar and the server's names.
func (s *RingSession) Completer() termio.Completer {
	names := newRingServerNames(s)
	return func(line string, pos int) (int, []string) {
		if pos > len(line) {
			pos = len(line)
		}
		start := pos
		for start > 0 && (ringWordByte(line[start-1])) {
			start--
		}
		return start, CompleteWith(names, line[:pos], s.selected)
	}
}

func ringWordByte(b byte) bool {
	return b == '_' || b == '-' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

// ringGreeting is the two plain lines a piped run begins with, as the legacy session prints.
func ringGreeting(out io.Writer) {
	_, _ = fmt.Fprintf(out, "hoardDB CLI %s\nType 'help' for help, 'exit' to quit.\n\n", version.Version)
}

// ringHintDatabases is how many databases the first-run hint asks for: enough to tell none from
// some from many. The call does work in proportion to the databases the session can see, never to buckets.
const ringHintDatabases = 4

// firstRunHint says what to type first: create a database on an empty server, pick one on a server that
// has some. It is empty when the server cannot be asked, so the greeting never carries an error.
func (s *RingSession) firstRunHint(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, ringNameCall)
	defer cancel()
	page, err := s.api.ShowDatabases(ctx, ringclient.PageRequest{Limit: ringHintDatabases})
	if err != nil {
		return ""
	}
	names := make([]string, 0, len(page.Databases))
	for _, database := range page.Databases {
		names = append(names, terminalSafe(database.Name))
	}
	switch {
	case len(names) == 0:
		return "No databases yet. Start with: create database app"
	case len(names) < ringHintDatabases && !page.More:
		return fmt.Sprintf("Databases: %s. Pick one with: use %s", strings.Join(names, ", "), names[0])
	}
	return fmt.Sprintf("Databases: %s, \u2026 ('show databases' for all). Pick one with: use %s", strings.Join(names[:min(len(names), ringHintDatabases-1)], ", "), names[0])
}

// greetInteractive is the greeting of a terminal session: the node's box (when boxed and the node can be
// asked) or the version line, what to type first, and how to get help. A piped run keeps ringGreeting, so a script's output does not change.
func (s *RingSession) greetInteractive(ctx context.Context, out io.Writer, boxed bool) {
	const helpLine = "Type 'help' to get started, 'exit' to quit.\n\n"
	if boxed {
		// A box needs a terminal at both ends, as in v1.0. A node that cannot answer describe_node (an older
		// server) gets v1.0's plain lines: a greeting is not a place for an error.
		if box, node, err := s.bannerBox(ctx, s.theme); err == nil {
			_, _ = fmt.Fprint(out, box)
			// A node that is not serving would refuse the hint's request anyway.
			if node.Serving == "" || node.Serving == "ok" {
				if hint := s.firstRunHint(ctx); hint != "" {
					_, _ = fmt.Fprintln(out, hint)
				}
			}
			_, _ = fmt.Fprint(out, helpLine)
			return
		}
	}
	_, _ = fmt.Fprintf(out, "hoardDB CLI %s\n", version.Version)
	if hint := s.firstRunHint(ctx); hint != "" {
		_, _ = fmt.Fprintln(out, hint)
	}
	_, _ = fmt.Fprint(out, helpLine)
}

func ringPrint(out io.Writer, output RingOutput) {
	for _, line := range output.Lines {
		_, _ = fmt.Fprintln(out, terminalSafeText(line))
	}
}

// RunPiped reads one statement per line from in, runs each and prints results to
// out. The first failure stops the run: its text goes to errOut as 'CLI error:
// <text>' and its exit status is returned, as the legacy piped session does.
// Blank lines and lines starting with '#' are skipped.
func (s *RingSession) RunPiped(ctx context.Context, in io.Reader, out, errOut io.Writer) int {
	ringGreeting(out)
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	// A new password follows its statement in the script, so the statement reads it
	// from this same scanner, as the legacy piped session does.
	s.script = func() (string, error) {
		if scanner.Scan() {
			return scanner.Text(), nil
		}
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
	defer func() { s.script = nil }()
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		output, err := s.Execute(ctx, line)
		ringPrint(out, output)
		if errors.Is(err, ErrExit) {
			_, _ = fmt.Fprintln(out, "Goodbye.")
			return ExitOK
		}
		if err != nil {
			_, _ = fmt.Fprintf(errOut, "%s %s\n", s.theme.errorPrefix(), terminalSafeText(err.Error()))
			return ringExit(err)
		}
	}
	if err := scanner.Err(); err != nil {
		_, _ = fmt.Fprintf(errOut, "CLI error: reading the statements: %v\n", err)
		return ExitUsage
	}
	return ExitOK
}

func ringExit(err error) int {
	var failed *RingError
	if errors.As(err, &failed) {
		return failed.Exit
	}
	return ExitRefused
}

// RunInteractive is the prompt: history, cursor editing and Tab completion on a
// terminal. A failed statement prints 'Error: <text>' and the session goes on.
func (s *RingSession) RunInteractive(ctx context.Context, out io.Writer, plain bool) int {
	// A terminal at both ends gets the box of v1.0, read from describe_node; a redirected stdout keeps the
	// plain lines, as in v1.0, and so does a node that cannot be asked.
	s.greetInteractive(ctx, out, !plain)
	s.interactive = true
	editor := termio.NewLineEditor(out, s.Completer())
	editor.SetPlain(plain)
	for {
		line, err := editor.ReadLine(s.Prompt())
		if err != nil {
			if errors.Is(err, termio.ErrEOF) {
				_, _ = fmt.Fprintln(out)
				return ExitOK
			}
			if errors.Is(err, termio.ErrInterrupt) {
				continue
			}
			_, _ = fmt.Fprintf(out, "Error: reading input: %v\n", err)
			return ExitUsage
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		editor.AddHistory(line)
		output, err := s.Execute(ctx, line)
		s.show(out, output)
		if errors.Is(err, ErrExit) {
			_, _ = fmt.Fprintln(out, "Goodbye.")
			return ExitOK
		}
		if err != nil {
			_, _ = fmt.Fprintf(out, "Error: %s\n", terminalSafeText(err.Error()))
		}
	}
}

// show prints what a statement printed, a screen at a time when it is longer
// than one and paging is on: the pager of v1.0 (pageText), on a terminal only.
func (s *RingSession) show(out io.Writer, output RingOutput) {
	if rows, ok := screenRows(out); ok && !s.noPager && terminalCanTakeEscapes() && len(output.Lines) >= rows {
		var text strings.Builder
		for _, line := range output.Lines {
			text.WriteString(terminalSafeText(line) + "\n")
		}
		if err := pageText(out, s.input, text.String(), rows); err != nil {
			_, _ = fmt.Fprintf(out, "paging: %v\n", err)
		}
		return
	}
	ringPrint(out, output)
}

// ringErrorIndent aligns the lines under an error with the text after 'Error: '.
const ringErrorIndent = 7

// ringErrorLines renders a failed statement for a prompt session: the problem on the first line and,
// when the error carries a fix, the fix on a line of its own, so it is visible without scrolling
// right. A piped run prints err.Error() on one line instead, so a script's output does not change.
func ringErrorLines(err error) []string {
	var parse *ParseError
	if errors.As(err, &parse) {
		if lines := ringParseLines(err, parse); lines != nil {
			return lines
		}
	}
	text := err.Error()
	cut := -1
	for _, marker := range []string{". Run '", ". Restart "} {
		if at := strings.Index(text, marker); at >= 0 && (cut < 0 || at < cut) {
			cut = at
		}
	}
	if cut < 0 {
		return ringHanging("Error: ", "", text)
	}
	return append(ringHanging("Error: ", "", text[:cut+1]), ringHanging(strings.Repeat(" ", ringErrorIndent), "Fix:   ", text[cut+2:])...)
}

// ringParseLines labels the parts of a refused statement: the problem, the form, an example and the
// page that lists every form. It reports nil for a refusal that has nothing to label.
func ringParseLines(err error, parse *ParseError) []string {
	if parse.Problem == "" {
		return nil
	}
	code := ""
	var failed *RingError
	if errors.As(err, &failed) && failed.Code != "" {
		code = failed.Code + ": "
	}
	indent := strings.Repeat(" ", ringErrorIndent)
	hint := ""
	if failed != nil {
		hint = strings.TrimPrefix(failed.Message, parse.Message)
	}
	problem := parse.Problem + "."
	if parse.Suggestion != "" {
		problem += " Did you mean " + parse.Suggestion + "?"
	}
	lines := ringHanging("Error: ", "", code+problem)
	switch {
	case parse.Form != "":
		lines = append(lines, ringHanging(indent, "The form:    ", parse.Form)...)
		lines = append(lines, ringHanging(indent, "For example: ", parse.Example)...)
		lines = append(lines, ringHanging(indent, "Every form:  ", parse.Topic)...)
	case parse.Example != "":
		lines = append(lines, ringHanging(indent, "For example: ", parse.Example)...)
	default:
		lines = append(lines, ringHanging(indent, "Fix:   ", "Run 'help' for the first five minutes, or 'help all' for every statement.")...)
	}
	if hint = strings.TrimSpace(hint); hint != "" {
		lines = append(lines, ringHanging(indent, "", hint)...)
	}
	return lines
}

// ringHanging wraps text after a prefix and a label into lines of at most 80 columns; the lines after
// the first line up under the text.
func ringHanging(prefix, label, text string) []string {
	lead := ringWidth(prefix) + ringWidth(label)
	var lines []string
	for i, line := range ringWrapWords(text, ringScreenWidth-lead) {
		if i == 0 {
			lines = append(lines, prefix+label+line)
			continue
		}
		lines = append(lines, strings.Repeat(" ", lead)+line)
	}
	if len(lines) == 0 {
		lines = append(lines, prefix+label)
	}
	return lines
}
