package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ringConnectExit maps a failed dial to the exit table: a refused login is
// ExitAuth (never retry), anything else is ExitConnect (nothing was sent).
func ringConnectExit(err error) int {
	if strings.Contains(err.Error(), "login") {
		return ExitAuth
	}
	return ExitConnect
}

// runRingSession is runSession for the ring engine. It dials through the
// shared ring dial (ringdial.go), which resolves the address, user and password
// exactly as the legacy session does and applies the trust rule, then runs the
// REPL or the piped statements.
func runRingSession(inv Invocation, flags *globalFlags, connection []CLIOption) int {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	client, addr, username, exit := dialRing(ctx, inv.Stderr, inv.Stdout, flags, connection)
	if client == nil {
		return exit
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil && !errors.Is(closeErr, ctx.Err()) {
			_, _ = fmt.Fprintf(inv.Stderr, "closing the connection: %v\n", closeErr)
		}
	}()
	stdoutTTY := false
	if file, ok := inv.Stdout.(*os.File); ok {
		info, statErr := file.Stat()
		stdoutTTY = statErr == nil && info.Mode()&os.ModeCharDevice != 0
	}
	_, _ = fmt.Fprintf(inv.Stdout, "Authenticated as %s\nConnected to %s\n", username, addr)
	options := []RingOption{WithRingTheme(resolveTheme(flags.theme, stdoutTTY)), WithRingPretty(flags.pretty), WithRingExtJSON(flags.extJSON), WithRingInput(inv.Stdin), WithRingIdentity(addr, username)}
	if flags.noPager {
		options = append(options, WithRingNoPager())
	}
	options = append(options, WithRingPasswordSource(flags.passwordFile, flags.passwordStdin, inv.Stderr))
	session := NewRingSession(NewRingAPI(client), options...)
	if inv.StdinIsTerminal {
		return session.RunInteractive(ctx, inv.Stdout, !stdoutTTY || !terminalCanTakeEscapes())
	}
	return session.RunPiped(ctx, inv.Stdin, inv.Stdout, inv.Stderr)
}
