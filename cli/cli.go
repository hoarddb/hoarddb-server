// Package cli provides the interactive command-line interface for hoardDB.
//
// The CLI connects to a hoardDB server over TCP+TLS (the driver listener,
// the tcp-protocol-spec record), reads user input from a REPL
// (Read-Eval-Print Loop), parses HQL commands, and dispatches them to the
// appropriate handler.
package cli

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/termio"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// CLI is the main command-line interface for hoardDB.
type CLI struct {
	addr           string
	currentDB      string
	token          string
	bearerToken    string
	tokenAddr      string
	tokenExpiresAt time.Time
	tokenDeadline  time.Time
	username       string
	noPrompt       bool
	noPager        bool
	password       string
	terminal       io.Writer
	// stdoutIsTTY is whether the session's output goes to a terminal. It is
	// resolved once at construction (from the writer, before any of it is
	// written) and decides two things: whether colour is emitted, and whether
	// the greeting is the box or the two plain lines. A redirected stdout must
	// not receive either the escapes or the frame.
	stdoutIsTTY bool
	// out is where a statement's results go. The command handler shares it, so
	// the session can hold a whole command's output and page it as one report
	// (see cli/pager.go).
	out *sessionOutput
	// chatter receives what the CLI says about itself while it works (the
	// insecure-TLS warning, the trust prompt, "Authenticated as", "Connected
	// to", debug lines) as opposed to the results of statements. It is the
	// terminal unless WithChatter says otherwise, so the REPL prints exactly
	// what it always did; a one-shot run points it at stderr. Commentary never
	// goes through out, so it reaches the user while a report is being held.
	chatter          io.Writer
	reader           io.Reader
	debug            bool
	insecureTLS      bool
	fingerprint      string
	knownServersPath string
	passwordFile     string
	passwordStdin    bool
	extendedJSON     bool
	pretty           bool
	handler          *CommandHandler
	conn             *transport.DriverClient

	// themeFlag is the --theme value (highest precedence); theme is the
	// resolved prompt theme (default, file, env or --theme). theme is resolved
	// once at construction, and its OSC background query runs at most once per
	// session.
	themeFlag string
	theme     *Theme
}

// CLIOption is a functional option for configuring the CLI.
type CLIOption func(*CLI)

// WithAddress sets the server address for the CLI.
func WithAddress(addr string) CLIOption {
	return func(c *CLI) {
		c.addr = addr
	}
}

// WithCredentials sets the username and password used for the AUTH
// handshake. The transport refuses every non-auth opcode until AUTH succeeds,
// so credentials are required for any command that talks to the server.
func WithCredentials(username, password string) CLIOption {
	return func(c *CLI) {
		c.username = username
		c.password = password
	}
}

// WithTLS is retained for callers that set it explicitly. The driver
// listener always runs over TLS 1.3, so there is nothing to enable or
// disable.
func WithTLS(enabled bool) CLIOption {
	return func(c *CLI) {
		_ = enabled
	}
}

// WithInsecureTLS disables server identity verification (the driver-spec record
// §3.2: "insecure mode a named loud option" — never a silent default). With
// this off (the default), Connect verifies the server's certificate against
// a persisted trust-on-first-use store and refuses a server whose key
// changed since the last connection; with this on, Connect accepts any
// certificate at all and prints a loud warning naming the address it just
// trusted blindly. Intended for local development against a server whose
// self-signed certificate is regenerated on every restart.
func WithInsecureTLS(enabled bool) CLIOption {
	return func(c *CLI) {
		c.insecureTLS = enabled
	}
}

// WithPinnedFingerprint trusts only the server whose certificate has this
// SHA256:<hex> fingerprint. Ring mode pins the node at -address this way, and
// the connection its user statements open to the same node must pin the same one.
func WithPinnedFingerprint(fingerprint string) CLIOption {
	return func(c *CLI) {
		c.fingerprint = fingerprint
	}
}

// WithNewPasswordSource configures where a user-creation password is read
// from: a 0600 file, or stdin. Neither ever puts the password on the command
// line, so it cannot land in shell history or ps output (spec 6.3).
func WithNewPasswordSource(file string, stdin bool) CLIOption {
	return func(c *CLI) {
		c.passwordFile = file
		c.passwordStdin = stdin
	}
}

// WithKnownServersPath overrides the default known_servers location
// (~/.hoarddb/known_servers, transport.KnownServersPath). Mainly useful for
// tests, and for a service account whose home directory isn't where server
// trust should live.
func WithKnownServersPath(path string) CLIOption {
	return func(c *CLI) {
		c.knownServersPath = path
	}
}

// WithExtendedJSON enables Extended JSON output for documents, which preserves
// type information (e.g. {"$date": "..."} for dates).
func WithExtendedJSON(on bool) CLIOption {
	return func(c *CLI) {
		c.extendedJSON = on
	}
}

// WithPretty lays each document out on several lines instead of one JSON object
// per line, for a person reading a document rather than a script parsing one.
func WithPretty(on bool) CLIOption {
	return func(c *CLI) {
		c.pretty = on
	}
}

// WithTheme overrides the theme file selection: a path to a theme.yml, or
// "plain" for no colour. It has the highest precedence in the chain.
func WithTheme(theme string) CLIOption {
	return func(c *CLI) {
		c.themeFlag = theme
	}
}

// WithDebug enables debug output.
func WithDebug(enabled bool) CLIOption {
	return func(c *CLI) {
		c.debug = enabled
	}
}

// WithTerminal sets the output writer for the CLI.
func WithTerminal(w io.Writer) CLIOption {
	return func(c *CLI) {
		c.terminal = w
	}
}

// WithChatter sends the CLI's own commentary (warnings, the trust prompt,
// "Authenticated as", "Connected to", debug lines) to w instead of the
// terminal, leaving statement results on the terminal. Without it both go to
// the terminal.
func WithChatter(w io.Writer) CLIOption {
	return func(c *CLI) {
		c.chatter = w
	}
}

// WithInput sets the input reader for the CLI.
func WithInput(r io.Reader) CLIOption {
	return func(c *CLI) {
		c.reader = r
	}
}

// WithNoPrompt makes the CLI refuse an unknown server's certificate instead of
// asking, even on a terminal. The verbs (dump, restore, ingest) and one-shot
// mode set it: a command that may run from a script must not stop to ask.
func WithNoPrompt(enabled bool) CLIOption {
	return func(c *CLI) {
		c.noPrompt = enabled
	}
}

// WithToken sets the AUTH resumption token. It is a bearer credential,
// equivalent to a password for anyone who can read it: presented instead of
// a password, kept in memory only, and never written by the CLI to disk.
// Also read from HOARDDB_AUTH_TOKEN.
func WithToken(token string) CLIOption {
	return func(c *CLI) {
		c.bearerToken = token
	}
}

// WithNoPager turns off paging for a session, for a user who would rather
// scroll their own terminal than press keys in ours.
func WithNoPager() CLIOption {
	return func(c *CLI) {
		c.noPager = true
	}
}

// NewCLI creates a new CLI instance with the given options.
func NewCLI(opts ...CLIOption) *CLI {
	c := &CLI{
		addr:     "localhost:7433",
		terminal: os.Stdout,
		reader:   os.Stdin,
	}
	for _, opt := range opts {
		opt(c)
	}
	// Read a bearer AUTH token from the environment if not set via option.
	if c.bearerToken == "" {
		c.bearerToken = os.Getenv("HOARDDB_AUTH_TOKEN")
	}
	// Read root credentials from environment if not set via option.
	if c.username == "" {
		c.username = os.Getenv("HOARDDB_ROOT_USER")
	}
	if c.password == "" {
		c.password = os.Getenv("HOARDDB_ROOT_PASSWORD")
	}
	// The handler prints a statement's results through the same sink as the CLI,
	// so one decision can hold and page a whole command's output.
	c.out = &sessionOutput{terminal: c.terminal}
	c.handler = NewCommandHandler(c.out, c.debug)
	if c.chatter == nil {
		// After every option has applied, so WithTerminal alone still moves
		// both streams.
		c.chatter = c.terminal
	} else {
		c.handler.SetChatter(c.chatter)
	}
	c.handler.SetCurrentDatabase(func() string { return c.currentDB })
	c.handler.SetReconnect(c.Reconnect)
	c.handler.SetPasswordSource(c.passwordFile, c.passwordStdin)
	c.handler.SetExtendedJSON(c.extendedJSON)
	c.handler.SetPretty(c.pretty)
	// The theme is resolved once per session, before the first prompt. The
	// TTY test is on stdout, not on stdin's interactive() predicate: a piped
	// session still prints the prompt, and a coloured prompt in a pipe would
	// leak escape bytes into transcripts.
	if f, ok := c.terminal.(*os.File); ok {
		if info, err := f.Stat(); err == nil {
			c.stdoutIsTTY = info.Mode()&os.ModeCharDevice != 0
		}
	}
	c.theme = resolveTheme(c.themeFlag, c.stdoutIsTTY)
	// Documents name their declared stores only where a person is reading: a
	// terminal at both ends, the gate the banner and the pager use.
	c.handler.SetDocumentDecoration(documentDecorationAllowed(c.stdoutIsTTY, c.interactive(), terminalCanTakeEscapes()))
	return c
}

// Run starts the REPL loop, reading commands from input and dispatching them.
//
// Commands fed from a pipe or file run non-interactively: the loop stops at
// the first failing command so a script cannot continue against a wrong
// assumption, and Run reports the failure so the process exits non-zero.
func (c *CLI) Run() error {
	// The OSC 11 background query runs once per session, before the first
	// prompt, and only when colour could actually be emitted: a piped session
	// with colour off would otherwise write a query nobody answers. Its reply
	// is drained in raw mode, so nothing lands in the first line typed.
	if c.theme != nil && c.theme.Colour {
		c.theme.Background = queryBackground()
		if c.theme.Background == "light" {
			lightVariant(c.theme)
		}
	}

	// The greeting. An interactive session gets the boxed summary of the node
	// it is connected to (cli/banner.go); a piped run keeps the two plain lines
	// it always had, so a script's stdout does not grow a frame it has to skip.
	// The release, not the field list: version.String() is the machine-readable
	// line -v and the startup log print, and embedding it here read as
	// "hoardDB CLI version=devel commit= build_date=". version.Version is the
	// tag, or the describe/sha of a dev build, which is what a user wants on
	// the first line of their session.
	scanner := bufio.NewScanner(c.reader)
	interactive := c.interactive()
	// The session's history is the editor's own list of what this session
	// typed: Up and Down recall it, and it goes when the session does. Nothing
	// is read from or written to a file — a statement names buckets, users and
	// keys, and there is no record of them left behind (owner decision,
	// 2026-10-11).
	editor := c.newEditor()
	if interactive && c.stdoutIsTTY {
		c.printBanner()
	} else {
		// A piped run keeps the two plain lines it always had — and so does a
		// session whose output is redirected while its input is a terminal: a
		// frame written into a file is a frame whatever the file is.
		c.printf("hoardDB CLI %s\n", version.Version)
		c.printf("Type 'help' for help, 'exit' to quit.\n\n")
	}

	readOne := func(prompt string) (string, error) {
		return c.readLine(scanner, editor, interactive, prompt)
	}
	if !interactive || editor == nil {
		c.handler.SetScriptLines(func() (string, error) {
			if scanner.Scan() {
				return scanner.Text(), nil
			}
			if err := scanner.Err(); err != nil {
				return "", err
			}
			return "", io.EOF
		})
		defer c.handler.SetScriptLines(nil)
	}
	linesRead := 0

	for {
		line, historyEntry, err := c.readStatement(readOne, &linesRead)
		if err != nil {
			if errors.Is(err, termio.ErrEOF) {
				c.printf("\n")
				return nil
			}
			if errors.Is(err, termio.ErrInterrupt) {
				// Ctrl-C abandons the line and keeps the session: leaving the
				// REPL on an interrupt is a worse default than a new prompt.
				continue
			}
			var unfinished *UsageError
			if errors.As(err, &unfinished) {
				return err
			}
			return fmt.Errorf("read input: %w", err)
		}
		// Only a completed statement is remembered: Ctrl-C on a half-typed
		// command must not put it in the history.
		if editor != nil && interactive {
			editor.AddHistory(historyEntry)
		}

		if err := c.handle(line); err != nil {
			if errors.Is(err, ErrExit) {
				c.printf("Goodbye.\n")
				return nil
			}
			if !interactive {
				return err
			}
			c.printf("Error: %v\n", err)
		}
	}
}

// readLine reads one input line. On a terminal it goes through the line
// editor, so the session has history, cursor editing and Tab completion; a
// non-interactive reader is scanned as before, and its errors keep the shape
// the scripted path has always reported.
func (c *CLI) readLine(scanner *bufio.Scanner, editor *termio.LineEditor, interactive bool, prompt string) (string, error) {
	if !interactive || editor == nil {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
				return "", err
			}
			return "", termio.ErrEOF
		}
		return scanner.Text(), nil
	}
	return editor.ReadLine(prompt)
}

// newEditor builds the line editor for an interactive session. It is nil when
// the input is not a terminal, so the editor never touches a pipe.
func (c *CLI) newEditor() *termio.LineEditor {
	if !c.interactive() {
		return nil
	}
	editor := termio.NewLineEditor(c.terminal, c.completer())
	// The editor redraws a line with cursor escapes. Where they cannot be
	// taken — a dumb or unknown terminal, or output that is a file or a pipe —
	// the line is read plain, so nothing but the prompt is ever written.
	editor.SetPlain(!c.stdoutIsTTY || !terminalCanTakeEscapes())
	return editor
}

// interactive reports whether input is a terminal. Scripted input (a pipe or
// a file) must fail fast instead of carrying on after an error.
func (c *CLI) interactive() bool {
	f, ok := c.reader.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// handle parses a command line and dispatches it to the appropriate handler.
func (c *CLI) handle(line string) error {
	cmd, err := ParseCommand(line)
	if err != nil {
		// The parser's error is the CLI's own verdict on the line, so it is a
		// usage error a script can tell from a refusal: PARSE_ERROR, exit 1.
		return fmt.Errorf("parse: %w", &UsageError{Message: err.Error(), Parse: true})
	}
	return c.runCommand(cmd)
}

// runCommand executes one parsed command, holding its output so that a report
// which does not fit on screen can be paged as a whole. Holding costs one
// buffer: output that fits is written straight through to the terminal.
func (c *CLI) runCommand(cmd *Cmd) error {
	rows, paging := c.pageable(cmd)
	if !paging {
		return c.dispatch(cmd)
	}
	release := c.out.hold()
	cmdErr := c.dispatch(cmd)
	held := release()
	if err := c.page(held, rows); err != nil {
		// page() has already printed the report in full; this says why it was
		// not paged, rather than leaving the user wondering why these lines
		// behaved differently from the last ones.
		c.notef("paging: %v\n", err)
	}
	return cmdErr
}

// dispatch runs one parsed command.
func (c *CLI) dispatch(cmd *Cmd) error {
	if cmd.Type == "global" && cmd.Method == "version" {
		// Offline verb: the same version string the -v flag and the banner
		// print, so an agent can ask in-band (GAP-4).
		_, _ = fmt.Fprintf(c.out, "hoardDB %s protocol=%d\n", version.String(), transport.ProtocolMajor)
		return nil
	}

	if cmd.Type == "global" && cmd.Method == "theme" {
		// Offline verb: no server call, prints the resolved theme and why
		// colour is or is not active, so a user can see what their file did.
		return c.printTheme()
	}

	if cmd.Type == "global" && cmd.Method == "show" && len(cmd.Args) == 1 && cmd.Args[0] == "banner" {
		// Presentation, not a statement: the box is built from the session's
		// theme, address, user and database, so the CLI draws it rather than
		// the handler. The greeting prints it once and it scrolls away; this
		// is how a user gets it back.
		return c.showBanner()
	}

	if cmd.Type == "global" && cmd.Method == "set" {
		// A session's output settings, changed while it runs (cli/settings.go):
		// they live on the CLI and on the handler, so the session applies them.
		return c.handleSet(cmd)
	}

	if cmd.Type == "global" && cmd.Method == "use" {
		// 'use' only takes effect once the server confirms the database
		// exists, so a typo leaves the session on its current database.
		if err := c.handler.Handle(cmd); err != nil {
			return err
		}
		c.currentDB = cmd.Database
		return nil
	}

	return c.handler.Handle(cmd)
}

// sessionState is what Reconnect must put back when the new server cannot be
// reached: the address, the live connection and the AUTH token minted for it.
type sessionState struct {
	addr           string
	conn           *transport.DriverClient
	token          string
	tokenAddr      string
	tokenExpiresAt time.Time
	tokenDeadline  time.Time
}

// Reconnect dials another server and, only when that succeeds, closes the
// current session. A failed dial or AUTH leaves the session on the previous
// server, still connected: closing first made a typo in the address
// destroy a working session.
func (c *CLI) Reconnect(addr string) error {
	if addr == "" {
		return usagef("usage: connect <host:port>")
	}
	previous := sessionState{
		addr: c.addr, conn: c.conn, token: c.token, tokenAddr: c.tokenAddr,
		tokenExpiresAt: c.tokenExpiresAt, tokenDeadline: c.tokenDeadline,
	}
	// Detach, do not close: the dial's failure path closes whatever c.conn
	// holds, and that must be the new connection only.
	c.conn = nil
	c.addr = addr
	if err := c.Connect(); err != nil {
		c.addr, c.conn, c.token, c.tokenAddr = previous.addr, previous.conn, previous.token, previous.tokenAddr
		c.tokenExpiresAt, c.tokenDeadline = previous.tokenExpiresAt, previous.tokenDeadline
		c.handler.SetConn(previous.conn)
		if previous.conn != nil {
			return fmt.Errorf("%w (the session is still connected to %s)", err, previous.addr)
		}
		return err
	}
	if previous.conn != nil {
		_ = previous.conn.Close()
	}
	return nil
}

// closeConn closes the current connection without clearing the address.
func (c *CLI) closeConn() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.handler.SetConn(nil)
	}
}

// Connect dials the server over TCP+TLS and authenticates the session.
//
// Authenticating is part of connecting rather than a lazily retried step: the
// transport rejects every non-auth opcode on an unauthenticated session, so
// without credentials no command could be served.
func (c *CLI) Connect() error {
	if err := c.dialAndAuthenticate(); err != nil {
		return err
	}
	c.notef("Connected to %s\n", c.addr)
	return nil
}

// buildTLSConfig builds the CLI's server-verification policy
// (the driver-spec record §3.2): by default, a persisted trust-on-first-use
// store with an interactive prompt on a server this client has never seen
// before, and a hard failure if a known server's key ever changes
// (transport.ClientVerifyPeerCertFunc already enforces the mismatch case —
// this only has to supply a real store and a real prompt). WithInsecureTLS
// is the named, loud escape hatch; without it, the CLI is the reference
// client other drivers copy behavior from, so it must not silently trust
// whatever certificate is presented, which is what passing a nil
// knownServers store to transport.MakeClientTLSConfig means.
func (c *CLI) buildTLSConfig() (*tls.Config, error) {
	if c.fingerprint != "" {
		return transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Expected: []string{c.fingerprint}}, c.addr)
	}
	if c.insecureTLS {
		c.notef("%s", insecureWarning(c.addr))
		return transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "--insecure flag"}}, c.addr)
	}

	path := c.knownServersPath
	if path == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("locate home directory for known_servers: %w", err)
		}
		path = transport.KnownServersPath(homeDir)
	}
	known, err := transport.NewKnownServers(path)
	if err != nil {
		return nil, fmt.Errorf("load known_servers: %w", err)
	}
	return transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Store: known, Prompt: c.promptTrust}, c.addr)
}

// promptTrust is the CLI's trust-on-first-use prompt: a server this client
// has never connected to before is not trusted silently. A non-interactive
// session (a script or pipe) has no operator to ask, so it refuses by
// default rather than guessing — the safe failure, since an operator who
// wants unattended trust can pre-populate known_servers or pass
// WithInsecureTLS explicitly.
func (c *CLI) promptTrust(host, fingerprint string) bool {
	c.notef("The authenticity of server '%s' cannot be established.\n", host)
	c.notef("ED25519 key fingerprint is %s.\n", fingerprint)
	if c.noPrompt || !c.interactive() {
		c.notef("Refusing to trust it non-interactively: pre-populate ~/.hoarddb/known_servers or pass --insecure to bypass.\n")
		return false
	}
	c.notef("Trust this server? [y/N]: ")
	scanner := bufio.NewScanner(c.reader)
	if !scanner.Scan() {
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(scanner.Text()))
	return answer == "y" || answer == "yes"
}

// authenticate performs the AUTH handshake. It resumes with a token when one
// is available — an explicit bearer token, or the session token minted for
// this same address and still inside its per-token lifetime — and falls back
// to the password exactly once on a TOKEN_* failure (spec 4 item 3c).
func (c *CLI) authenticate() error {
	bearer := c.bearerToken
	if bearer == "" && c.token != "" && c.tokenAddr == c.addr &&
		(c.tokenExpiresAt.IsZero() || time.Now().Before(c.tokenExpiresAt)) {
		bearer = c.token
	}
	if bearer != "" {
		err := c.authWithToken(bearer)
		if err == nil {
			c.notef("Authenticated as %s (resumed by token)\n", c.username)
			return nil
		}
		// Only a TOKEN_* failure earns one password retry; a credential or
		// rate-limit failure must surface. A password-less pool cannot fall
		// back, so it reports the token failure plainly.
		if !isTokenFallback(err) || c.username == "" || c.password == "" {
			return err
		}
		c.notef("Token resume failed; falling back to password\n")
	}
	return c.authWithPassword()
}

func (c *CLI) authWithToken(token string) error {
	var resp server.AuthResponse
	if err := c.handler.send(transport.OpAuth, &server.AuthRequest{Token: token}, &resp); err != nil {
		return err
	}
	if !resp.Success {
		return &RefusedError{Message: "server rejected the AUTH token without a reason"}
	}
	c.recordAuth(resp)
	return nil
}

func (c *CLI) authWithPassword() error {
	if c.username == "" {
		return errNoCredentials()
	}

	var resp server.AuthResponse
	req := &server.AuthRequest{Username: c.username, Password: c.password}
	if err := c.handler.send(transport.OpAuth, req, &resp); err != nil {
		return fmt.Errorf("authenticate as %s: %w", c.username, err)
	}
	if !resp.Success {
		return refusedf("authenticate as %s: server rejected the credentials", c.username)
	}

	c.recordAuth(resp)
	c.notef("Authenticated as %s\n", c.username)
	return nil
}

// recordAuth stores the minted token and both expiry numbers so a reconnect
// can decide whether a resume is still worth trying, and the server's password
// minimum, which a new password is checked against.
func (c *CLI) recordAuth(resp server.AuthResponse) {
	c.token = resp.Token
	c.tokenAddr = c.addr
	c.handler.passwordMinLength = int(resp.PasswordMinLength)
	c.tokenExpiresAt = time.Time{}
	c.tokenDeadline = time.Time{}
	if resp.TokenExpiresAt > 0 {
		c.tokenExpiresAt = time.Unix(resp.TokenExpiresAt, 0)
	}
	if resp.TokenDeadline > 0 {
		c.tokenDeadline = time.Unix(resp.TokenDeadline, 0)
	}
}

// isTokenFallback reports whether err is a TOKEN_* failure that a driver
// should answer once with a password.
func isTokenFallback(err error) bool {
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		return false
	}
	switch authErr.Reason {
	case transport.AuthFailTokenExpired, transport.AuthFailTokenInvalid, transport.AuthFailTokenUnsupported:
		return true
	}
	return false
}

// Close closes the driver connection.
func (c *CLI) Close() {
	c.closeConn()
}

// ErrorPrefix returns the "CLI error:" label rendered in the theme's error
// style, or plain when colour is off. cmd/cli/main.go prints it to stderr
// when the REPL fails.
func (c *CLI) ErrorPrefix() string { return c.theme.errorPrefix() }

// errorPrefix is the "CLI error:" label in the theme's error style; a nil or
// colourless theme gives it plain. Ring mode prints the same label.
func (t *Theme) errorPrefix() string {
	label := "CLI error:"
	if t != nil && t.Colour {
		if style, ok := t.Styles["error"]; ok {
			return coloured(label, style, true)
		}
	}
	return label
}

// insecureWarning is the line printed when certificate verification is off,
// the same in both modes.
func insecureWarning(addr string) string {
	return fmt.Sprintf("WARNING: TLS certificate verification is disabled (--insecure); the connection to %s cannot be trusted and may be intercepted. Use this only against a local development server.\n", addr)
}

// prompt returns the current prompt string, rendered from the resolved
// theme. The theme renders %database and the styled spans; when colour is off
// (pipe, NO_COLOR, TERM=dumb, or a plain theme) the same format renders with
// no escapes at all.
func (c *CLI) prompt() string {
	if c.theme == nil {
		// A CLI built without the theme wiring (tests, unusual options) falls
		// back to the plain historical prompt.
		if c.currentDB != "" {
			return fmt.Sprintf("hoardDB [%s]> ", c.currentDB)
		}
		return "hoardDB> "
	}
	return c.theme.renderPrompt(c.currentDB)
}

// printTheme implements the `theme` REPL verb: the resolved theme name, the
// file it came from, whether colour is active and why not, the detected
// background, and the rendered prompt — all offline.
func (c *CLI) printTheme() error {
	if c.theme == nil {
		_, _ = fmt.Fprintf(c.out, "theme: built-in default\n")
		return nil
	}
	t := c.theme
	colour := "yes"
	if !t.Colour {
		colour = "no"
		if t.ColourReason != "" {
			colour += " (" + t.ColourReason + ")"
		}
	}
	_, _ = fmt.Fprintf(c.out,
		"name:       %s\n"+
			"source:     %s\n"+
			"colour:     %s\n"+
			"background: %s\n"+
			"prompt:     %s\n",
		t.Name, t.Source, colour, backgroundLabel(t.Background), t.renderPrompt(c.currentDB))
	return nil
}

// backgroundLabel renders the detected background for the theme verb.
func backgroundLabel(bg string) string {
	if bg == "" {
		return "unknown (no OSC 11 reply)"
	}
	return bg
}

func (c *CLI) printf(format string, args ...interface{}) {
	_, _ = io.WriteString(c.out, neutralizeControls(fmt.Sprintf(format, args...)))
}

// notef prints commentary about the session, as opposed to a statement's
// result, to the chatter writer.
func (c *CLI) notef(format string, args ...interface{}) {
	_, _ = io.WriteString(c.chatter, neutralizeControls(fmt.Sprintf(format, args...)))
}
