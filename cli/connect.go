package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ConnectSpec is everything that decides which server the CLI reaches and as
// whom. The REPL, dump, restore and ingest each fill one and call
// connectSession, so the address precedence, the credential precedence and the
// error types are written once (GAP-24 Part 8a).
type ConnectSpec struct {
	// Address is the flag's value. It yields to the configuration file's TCP
	// listen address unless ExplicitAddress is set.
	Address         string
	ExplicitAddress bool
	// Config is the path of a YAML configuration file, or empty.
	Config string
	// AuthToken is a bearer credential. User and Password fall back to the
	// configuration file, then to the environment inside NewCLI.
	AuthToken, User, Password string
	Insecure                  bool
	// Fingerprints are pinned server fingerprints. The ring dial (ringdial.go) pins the
	// one the user gave; the resolver here only builds the credentials and address.
	Fingerprints []string
	// NoPrompt makes an unknown server's certificate a refusal instead of a
	// question.
	NoPrompt bool
	// Chatter receives connection notes ("Authenticated as ..."). Nil leaves the
	// CLI's own default, which the REPL sets with WithTerminal.
	Chatter io.Writer
	// Stdin is where a trust question would be answered. Nil leaves os.Stdin.
	Stdin io.Reader
	// Getenv reads the environment. It is required.
	Getenv func(string) string
}

// resolveConnection turns a spec into the options NewCLI takes. It is the one
// place that loads the configuration file and applies the precedence rules.
func resolveConnection(spec ConnectSpec) ([]CLIOption, error) {
	// The refusal of the old replicated mode belongs to a server start; a client reads the same
	// environment and must still connect.
	cfg, err := config.LoadAllowingOldReplicatedMode(spec.Config)
	if err != nil {
		return nil, usagef("failed to load config: %v", err)
	}

	// The CLI dials the TCP driver port (cfg.TCP.Listen), not the internode
	// Server.Listen address. An explicit address wins over the config file.
	address := spec.Address
	if !spec.ExplicitAddress && cfg.TCP.Listen != "" {
		address = cfg.TCP.Listen
	}
	options := []CLIOption{
		WithAddress(address), WithInsecureTLS(spec.Insecure), WithNoPrompt(spec.NoPrompt),
	}
	if spec.Chatter != nil {
		options = append(options, WithTerminal(spec.Chatter))
	}
	if spec.Stdin != nil {
		options = append(options, WithInput(spec.Stdin))
	}
	if spec.AuthToken != "" {
		options = append(options, WithToken(spec.AuthToken))
	}
	// The server refuses every non-auth opcode until AUTH succeeds, so without a
	// user and password the CLI could connect but not run a command. Flags win
	// over the configuration file, which wins over the environment.
	user, password := spec.User, spec.Password
	if user == "" {
		user = cfg.Server.RootUser
	}
	if password == "" {
		password = cfg.Server.RootPassword
	}
	if user != "" && password != "" {
		options = append(options, WithCredentials(user, password))
	}
	return options, nil
}

// connectSession resolves the spec, builds a CLI and runs the two-phase dial.
//
// The CLI is returned even when the dial fails, so the caller can name the
// address it tried; it is open only when the error is nil. It is nil when the
// spec itself was refused (a bad configuration file, an unsupported flag).
func connectSession(spec ConnectSpec) (*CLI, error) {
	options, err := resolveConnection(spec)
	if err != nil {
		return nil, err
	}
	session := NewCLI(options...)
	return session, session.dialAndAuthenticate()
}

// dialAndAuthenticate connects in two phases that fail differently, so a script
// can tell "could not reach the server" (exit 6) from "the server refused these
// credentials" (exit 7). It is the single implementation behind CLI.Connect,
// the verbs and one-shot mode.
func (c *CLI) dialAndAuthenticate() error {
	// Before any dial: a session with nothing to authenticate with cannot do
	// anything, and a trust question asked first is a question for nothing.
	if c.bearerToken == "" && c.token == "" && c.username == "" {
		return errNoCredentials()
	}
	tlsConfig, err := c.buildTLSConfig()
	if err != nil {
		return &ConnectError{Phase: "tls", Address: c.addr, Err: err}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := transport.NewDriverClient(ctx, c.addr, tlsConfig)
	if err != nil {
		return &ConnectError{Phase: "dial", Address: c.addr, Err: err}
	}
	c.conn = conn
	c.handler.SetConn(conn)
	if err := c.authenticate(); err != nil {
		c.closeConn()
		return c.classifyAuthenticateFailure(err)
	}
	return nil
}

// classifyAuthenticateFailure types an AUTH failure by what it is, never by its
// text. A rejection stays an *AuthError (exit 7). An error frame the server
// sent in answer to AUTH, at the connection limit for one, is a connect
// failure: nothing was wrong with the credentials. A missing credential is the
// caller's usage error. Anything else did not come from the server's answer, so
// the connection itself failed.
func (c *CLI) classifyAuthenticateFailure(err error) error {
	var authErr *AuthError
	var usage *UsageError
	var serverErr *ServerError
	switch {
	case errors.As(err, &authErr), errors.As(err, &usage):
		return err
	case errors.As(err, &serverErr):
		return &ConnectError{Phase: "auth-handshake", Address: c.addr, Err: err}
	}
	return &ConnectError{Phase: "dial", Address: c.addr, Err: err}
}

// errNoCredentials is the usage error for a session with no user, password or
// token anywhere.
func errNoCredentials() error {
	return usagef(`no credentials
       pass -user <name> -password <password>, or set the environment:
         HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=<password> hoarddb
       the server's root user comes from HOARDDB_ROOT_USER/HOARDDB_ROOT_PASSWORD
       when the server is started`)
}

// verbConnectFailure prints a failed connectSession the way dump and restore
// have always printed it and returns their exit number. Part 8b moves both
// onto classify; until then this keeps their pinned numbers.
func verbConnectFailure(stderr io.Writer, session *CLI, err error) int {
	var usage *UsageError
	if errors.As(err, &usage) {
		_, _ = fmt.Fprintln(stderr, terminalSafeText(err.Error()))
		return ExitUsage
	}
	_, _ = fmt.Fprintf(stderr, "failed to connect to %s: %s\n", session.addr, terminalSafeText(err.Error()))
	return ExitRefused
}

// fingerprintFlag collects the repeatable --fingerprint flag.
type fingerprintFlag []string

func (f *fingerprintFlag) String() string { return strings.Join(*f, ",") }

func (f *fingerprintFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}
