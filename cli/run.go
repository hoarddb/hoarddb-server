package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/termio"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// Invocation is everything the CLI binary touches outside its own arguments,
// so a test can run it without a process: no os.Args, no os.Exit, no real
// terminal. RunMain returns the exit code; only cmd/cli passes it to os.Exit.
type Invocation struct {
	// Args are the command-line arguments after the program name.
	Args []string
	// Stdin reaches the statement reader. A password for `create user` in a
	// piped session is read from that same reader (CommandHandler.scriptLines);
	// only a terminal prompt reads the process's own stdin, through termio.
	Stdin io.Reader
	// Stdout carries results; Stderr carries diagnostics and usage.
	Stdout, Stderr io.Writer
	// StdinIsTerminal is decided by the caller so the mode rules (REPL versus
	// piped statements) are testable without a pty.
	StdinIsTerminal bool
	// Getenv reads the environment, for the same reason.
	Getenv func(string) string
	// Signals delivers interrupts. The REPL and piped mode ignore it today; the
	// one-shot path reads it between statements.
	Signals <-chan os.Signal
	// Exit ends the process from inside a verb that must not wait (a second
	// signal). RunMain never calls it; it is the seam cli/ingest.go:ingestEnv
	// already has, carried here so the verbs can take it from the Invocation.
	Exit func(int)
}

// ProcessInvocation builds the Invocation of the running process. It is the one
// place in this package that reads os.Args and names os.Exit.
//
// Signals is nil on purpose. Registering signal.Notify here would make Ctrl-C a
// no-op in the REPL and in piped mode, because nothing reads the channel until
// the one-shot path does (GAP-24 Part 14), and a hung statement would no longer
// be interruptible. The one-shot path registers for itself.
func ProcessInvocation() Invocation {
	stdinInfo, err := os.Stdin.Stat()
	return Invocation{
		Args:            os.Args[1:],
		Stdin:           os.Stdin,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
		StdinIsTerminal: err == nil && stdinInfo.Mode()&os.ModeCharDevice != 0,
		Getenv:          os.Getenv,
		Exit:            os.Exit,
	}
}

// verbRunners are the standalone verbs, each a complete command with its own
// flag set. They take the arguments after the verb and, for now, use the
// process's own streams (GAP-24 Part 8b converts them to the Invocation).
var verbRunners = map[string]func(args []string) int{
	"dump":    RunRingDumpCommand,
	"restore": restoreEntry,
	"ingest":  RunRingIngestCommand,
	"plan":    RunPlanCommand,
	"seed":    RunSeedCommand,
}

// globalFlags are the flags of a session (REPL or piped statements).
type globalFlags struct {
	address        string
	debug          bool
	configPath     string
	authToken      string
	user           string
	password       string
	passwordPrompt bool
	insecure       bool
	passwordFile   string
	passwordStdin  bool
	extJSON        bool
	pretty         bool
	theme          string
	noPager        bool
	version        bool
	fingerprints   fingerprintFlag
}

// RunMain runs the CLI for one invocation and returns the process exit code.
//
// A verb (dump, restore, ingest) is dispatched only when it is the first
// argument and before any flag is parsed: a verb that fell through into the
// REPL would read stdin until EOF and hang a pipeline instead of failing
// (the dump-restore-build record, trap 9). Everything else is a
// session.
func RunMain(inv Invocation) int {
	if len(inv.Args) > 0 {
		if runVerb, isVerb := verbRunners[inv.Args[0]]; isVerb {
			return runVerb(inv.Args[1:])
		}
	}

	// A private FlagSet with ContinueOnError, never flag.CommandLine: the
	// default set exits the process with 2 on a bad flag, and 2 means "the
	// server refused" everywhere else in this binary's exit table.
	fs := flag.NewFlagSet("hoarddb", flag.ContinueOnError)
	fs.SetOutput(inv.Stderr)
	flags := registerGlobalFlags(fs)
	fs.Usage = func() { printUsage(fs) }
	if err := fs.Parse(passwordWithValue(inv.Args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK
		}
		// The flag package has already printed the error and the usage.
		return ExitUsage
	}

	// The version flag prints and returns before dialing, before reading a
	// password, before any connection: the CLI must be able to report its
	// build from a machine with no server on it.
	if flags.version {
		_, _ = fmt.Fprintln(inv.Stdout, versionLine("hoardDB"))
		return ExitOK
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(inv.Stderr, leftoverArgumentsMessage(fs.Arg(0)))
		return ExitUsage
	}
	if flags.passwordFile != "" && flags.passwordStdin {
		_, _ = fmt.Fprintln(inv.Stderr, "--password-file and --password-stdin are mutually exclusive")
		return ExitUsage
	}
	return runSession(inv, fs, flags)
}

// passwordWithValue keeps `-password VALUE` and `--password VALUE` working
// beside the bare prompt: a value that follows the flag is rewritten to the
// `=VALUE` form the flag package reads. A bare -password, or one followed by
// another flag, still prompts. A password that starts with "-" needs the
// `=VALUE` form.
func passwordWithValue(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if (arg == "-password" || arg == "--password") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			out = append(out, arg+"="+args[i+1])
			i++
			continue
		}
		out = append(out, arg)
	}
	return out
}

// passwordFlag is -password. The flag package lets a boolean-style flag stand
// without a value, and Set receives "true" for a bare -password, which asks for
// the password instead, as mysql and mongo do. A literal password of "true" has
// to come from the environment or a prompt.
type passwordFlag struct{ flags *globalFlags }

func (p *passwordFlag) String() string {
	if p == nil || p.flags == nil {
		return ""
	}
	return p.flags.password
}

func (p *passwordFlag) IsBoolFlag() bool { return true }

func (p *passwordFlag) Set(value string) error {
	if value == "true" {
		p.flags.passwordPrompt = true
		return nil
	}
	p.flags.password = value
	return nil
}

// addressFlagUsage is the -address help of the session, dump, restore and
// ingest. The flag's own default is the last resort: resolveConnection replaces
// it with the loaded configuration's tcp.listen unless -address was given, and
// the configuration always has one, so the usual default is that address. The
// built-in value is read from config.DefaultConfig so the help cannot drift
// from it.
var addressFlagUsage = fmt.Sprintf("hoardDB server address (TCP driver port). Without it the CLI dials the tcp.listen "+
	"address of the loaded configuration (env HOARDDB_TCP_LISTEN; %s when nothing is configured), "+
	"and this default only if that is empty", config.DefaultConfig().TCP.Listen)

// registerGlobalFlags declares every flag of a session. Names, defaults and
// help text are the ones cmd/cli/main.go declared before RunMain existed.
func registerGlobalFlags(fs *flag.FlagSet) *globalFlags {
	flags := &globalFlags{}
	fs.StringVar(&flags.address, "address", "localhost:7433", addressFlagUsage)
	fs.BoolVar(&flags.debug, "debug", false, "enable debug output")
	fs.StringVar(&flags.configPath, "config", "", "path to YAML configuration file")
	fs.StringVar(&flags.authToken, "auth-token", "", "AUTH resumption token, a bearer credential (env: HOARDDB_AUTH_TOKEN)")
	fs.StringVar(&flags.user, "user", "", "root username (env: HOARDDB_ROOT_USER)")
	fs.Var(&passwordFlag{flags: flags}, "password", "root password: a bare -password asks for it on a terminal, -password=VALUE gives it on the command line (env: HOARDDB_ROOT_PASSWORD)")
	fs.BoolVar(&flags.insecure, "insecure", false, "skip TLS certificate verification (local development only — the connection cannot be trusted)")
	fs.Var(&flags.fingerprints, "fingerprint", "pin the server's certificate fingerprint, SHA256:<64 hex>, which the server prints when it starts")
	fs.StringVar(&flags.passwordFile, "password-file", "", "read the new password for user creation from this 0600 file (never a command-line password)")
	fs.BoolVar(&flags.passwordStdin, "password-stdin", false, "read the new password for user creation from the line after the statement in a piped script")
	fs.BoolVar(&flags.extJSON, "extjson", false, "print documents as Extended JSON (relaxed) so dates keep their type")
	fs.BoolVar(&flags.pretty, "pretty", false, "print each document on several lines instead of one JSON object per line")
	fs.StringVar(&flags.theme, "theme", "", "prompt theme: a path to a theme.yml, or 'plain' for no colour; 'help theme' lists the file's keys (env: HOARDDB_THEME)")
	fs.BoolVar(&flags.noPager, "no-pager", false, "print long reports in full instead of a screen at a time (a piped run never pages)")
	// Go's flag package treats --version as the same flag as -version, so a
	// single registration per name covers -v, -version and --version.
	fs.BoolVar(&flags.version, "v", false, "print the version and exit")
	fs.BoolVar(&flags.version, "version", false, "print the version and exit")
	return flags
}

// printUsage is the -h text: what the binary is, runnable examples, then the
// flags. It is also what a flag error prints after the error line.
func printUsage(fs *flag.FlagSet) {
	output := fs.Output()
	_, _ = fmt.Fprint(output, `Usage: hoarddb [flags]
       hoarddb dump|restore|ingest|plan|seed [flags] ...   (a verb must come first)

With no verb, hoarddb opens an interactive session, or runs the statements
piped on its standard input.

Examples:
  hoarddb -address 127.0.0.1:7433 -insecure                           start an interactive session; try 'help'
  echo 'show databases' | hoarddb -address 127.0.0.1:7433 -insecure   run piped statements
  hoarddb dump --out app.hoarddump                                    back up every database you may see
  hoarddb restore app.hoarddump                                       restore it
  hoarddb ingest users.bson --into app.users                          load a file into a bucket
  hoarddb plan users.bson posts.bson --no-input                       suggest a design from your data
  hoarddb seed agent --insecure                                       load an example data set from the how-to pages
  hoarddb dump -h                                                     the flags of one verb

Flags:
`)
	fs.PrintDefaults()
}

// leftoverArgumentsMessage is the usage error for a word that is neither a
// flag nor a verb. It names both things the user most likely meant (spec F12).
// The statement half says "pipe it" because -c is not built yet (GAP-24
// Part 10); the message then points to -c.
func leftoverArgumentsMessage(word string) string {
	if _, isVerb := verbRunners[word]; isVerb {
		return fmt.Sprintf("a verb must come first: hoarddb %s ... (flags go after the verb, not before it)", word)
	}
	return fmt.Sprintf("hoarddb takes no positional arguments, and %q is not a verb. "+
		"To run a statement, pipe it on stdin (echo '<statement>' | hoarddb ...); "+
		"to run a verb, put it first (hoarddb dump ...)", word)
}

// runSession starts the REPL, or the piped-statement loop when stdin is not a
// terminal; CLI.Run decides which from its input.
func runSession(inv Invocation, fs *flag.FlagSet, flags *globalFlags) int {
	installClientLogger(flags.debug)
	if flags.passwordPrompt {
		// The statements may be piped on standard input, so a prompt is only
		// asked where a person can answer it.
		if !termio.StdinIsTerminal() {
			_, _ = fmt.Fprintln(inv.Stderr, "-password needs a terminal to ask for the password, and standard input here is not one. "+
				"In a script, set HOARDDB_ROOT_PASSWORD, or give the password as -password=VALUE, which shows it in ps.")
			return ExitUsage
		}
		password, err := termio.ReadPassword("Password: ")
		if err != nil {
			_, _ = fmt.Fprintln(inv.Stderr, err)
			return ExitUsage
		}
		flags.password = password
	}
	spec := ConnectSpec{
		Address: flags.address, ExplicitAddress: flagWasProvided(fs, "address"), Config: flags.configPath,
		AuthToken: flags.authToken, User: flags.user, Password: flags.password,
		Insecure: flags.insecure,
		Stdin:    inv.Stdin, Getenv: inv.Getenv,
	}
	// The ring session pins the fingerprint itself (ringdial.go), so the connection
	// resolver is given none.
	options, err := resolveConnection(spec)
	if err != nil {
		_, _ = fmt.Fprintln(inv.Stderr, terminalSafeText(err.Error()))
		return classify(err).Exit
	}
	return runRingSession(inv, flags, options)
}

// insecureTrustRecord is the transport's own structured record of the same fact
// the CLI states as a one-line warning before it connects. It is the one record
// the CLI drops: printing it would say the same thing twice, the second time as
// a timestamp and key=value pairs, which is not a shape a user can act on.
const insecureTrustRecord = "TLS certificate verification disabled by an explicit insecure trust policy"

// installClientLogger owns the stream the client's library layer logs to.
//
// Nothing installs a default slog logger, so a Warn from transport/ printed on
// Go's standard logger beside the CLI's own one-line WARNING. The CLI owns that
// stream, and what it does with it is narrow: the one record it has already said
// itself is dropped, and everything else the library logs at warnings and above
// still reaches the user. Discarding the lot was the bug — a fingerprint that
// could not be recorded is a real warning and it was going nowhere.
//
// The session calls it with its -debug flag, which turns the whole stream on,
// duplicate included; the verbs (dump, restore, ingest) call it with false,
// because they carry no debug flag — if one of them grows one, the flag is the
// argument to pass. GAP-24's RunMain consolidation is where a shared -debug
// belongs.
func installClientLogger(debug bool) {
	slog.SetDefault(slog.New(clientLogHandler(os.Stderr, debug)))
}

// clientLogHandler is where a client library's records go: stderr, at warnings
// and above, and at every level when -debug asked for them.
func clientLogHandler(w io.Writer, debug bool) slog.Handler {
	level := slog.LevelWarn
	if debug {
		level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})
	if debug {
		// The duplicate is worth having when someone is debugging the
		// connection, which is the only reason the stream is on at all.
		return handler
	}
	return &duplicateRecordHandler{next: handler}
}

// duplicateRecordHandler forwards every record except the insecure-trust one the
// CLI prints itself.
type duplicateRecordHandler struct {
	next slog.Handler
}

func (h *duplicateRecordHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *duplicateRecordHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == insecureTrustRecord {
		return nil
	}
	return h.next.Handle(ctx, record)
}

func (h *duplicateRecordHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &duplicateRecordHandler{next: h.next.WithAttrs(attrs)}
}

func (h *duplicateRecordHandler) WithGroup(name string) slog.Handler {
	return &duplicateRecordHandler{next: h.next.WithGroup(name)}
}

// versionLine renders the -v output for the CLI, the same shape the server
// prints, so the two binaries agree on what a version looks like.
func versionLine(name string) string {
	return fmt.Sprintf("%s %s protocol=%d", name, version.String(), transport.ProtocolMajor)
}

// flagWasProvided reports whether the command line named a flag explicitly,
// as opposed to leaving it at its default.
func flagWasProvided(fs *flag.FlagSet, name string) bool {
	provided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}
