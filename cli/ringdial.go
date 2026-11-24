package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringDialTimeout bounds the ring dial: the TCP and TLS connect and the AUTH
// round trip. The same bound the legacy session uses.
const ringDialTimeout = 20 * time.Second

// ringTrustRefusal is the one text for a ring dial that trusts no server. The
// session and the verbs share it, so the two cannot drift apart.
const ringTrustRefusal = "The CLI trusts no server by default. Pass -fingerprint SHA256:<hex> (the server prints it when it starts), " +
	"or -insecure for a node you started yourself on this machine"

// ringFingerprintTwiceText refuses a pin on more than one server: a ring
// session talks to one node.
const ringFingerprintTwiceText = "-fingerprint is given more than once, and the CLI pins one server: give the fingerprint of the node at -address"

// ringRedial re-dials the ring with the options of the first dial, for a sender
// whose connection died mid-run. The first client is finished for good
// (ringclient.Client.fail), so a sender that must resend a frame takes a new
// client from here and keeps its request id (build plan 3.4 step 6).
type ringRedial func(ctx context.Context) (*ringclient.Client, error)

// ringFingerprint picks the one pinned fingerprint out of the repeatable flag.
// It reports false after printing the refusal when the flag was given twice.
func ringFingerprint(stderr io.Writer, fingerprints []string) (string, bool) {
	if len(fingerprints) > 1 {
		_, _ = fmt.Fprintln(stderr, ringFingerprintTwiceText)
		return "", false
	}
	if len(fingerprints) == 1 {
		return fingerprints[0], true
	}
	return "", true
}

// dialRingTrusted applies ring mode's trust rule to dial and dials once. It
// returns the open client and ExitOK, or nil and the exit code of the refusal:
// a trust rule that pins nothing is ExitConnect before any byte is sent, and a
// failed dial is classified by ringConnectExit. Refusals go to stderr; the
// insecure warning goes to notice, which is the stream the caller owns for it
// (the session's transcript on stdout, a verb's report on stderr).
func dialRingTrusted(ctx context.Context, stderr, notice io.Writer, dial ringclient.Options) (*ringclient.Client, int) {
	if dial.Fingerprint == "" && len(dial.Fingerprints) == 0 && !dial.Insecure {
		_, _ = fmt.Fprintln(stderr, ringTrustRefusal)
		return nil, ExitConnect
	}
	if dial.Insecure {
		_, _ = fmt.Fprint(notice, insecureWarning(dial.Addr))
	}
	dialCtx, cancel := context.WithTimeout(ctx, ringDialTimeout)
	defer cancel()
	client, err := ringclient.Dial(dialCtx, dial)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Failed to connect to %s: %s\n", dial.Addr, terminalSafeText(strings.TrimPrefix(err.Error(), "ringclient: ")))
		return nil, ringConnectExit(err)
	}
	return client, ExitOK
}

// dialRing is the ring dial of a session. It resolves the credentials from the
// connection options, applies the trust rule, dials, and returns the client,
// the address it dialed, the user it authenticated as and an exit code that is
// ExitOK when the client is open. The caller prints the authenticated banner;
// refusals and a failed dial go to stderr, the insecure warning to notice, the
// session's stdout, where its transcript has always carried it.
func dialRing(ctx context.Context, stderr, notice io.Writer, flags *globalFlags, connection []CLIOption) (*ringclient.Client, string, string, int) {
	resolved := NewCLI(connection...)
	if resolved.username == "" || resolved.password == "" {
		_, _ = fmt.Fprintln(stderr, terminalSafeText(errNoCredentials().Error()))
		return nil, "", "", ExitUsage
	}
	fingerprint, ok := ringFingerprint(stderr, flags.fingerprints)
	if !ok {
		return nil, "", "", ExitUsage
	}
	dial := ringclient.Options{
		Addr: resolved.addr, User: resolved.username, Password: resolved.password,
		Insecure: flags.insecure, Fingerprint: fingerprint,
	}
	client, exit := dialRingTrusted(ctx, stderr, notice, dial)
	return client, dial.Addr, resolved.username, exit
}

// dialRingForVerb is dialRing for a standalone verb. It resolves the verb's
// connection flags the way the legacy verbs do (the flags, then the
// configuration file, then the environment), dials the ring, and returns the
// client, a redial for a connection that dies mid-run, and the exit code. The
// notices and the insecure warning go to stderr: a verb's stdout is its report.
func dialRingForVerb(ctx context.Context, stderr io.Writer, fs *flag.FlagSet, connection *connectionFlags) (*ringclient.Client, ringRedial, int) {
	client, _, redial, exit := dialRingForVerbUser(ctx, stderr, fs, connection)
	return client, redial, exit
}

// dialRingForVerbUser is dialRingForVerb with the name the session
// authenticated as: a dump's header records which user's view its file holds,
// and its report says the same, so the dial that resolved the credentials is
// the one place that knows (section 3.2 item 1).
func dialRingForVerbUser(ctx context.Context, stderr io.Writer, fs *flag.FlagSet, connection *connectionFlags) (*ringclient.Client, string, ringRedial, int) {
	client, user, _, redial, exit := dialRingVerb(ctx, stderr, fs, connection, false)
	return client, user, redial, exit
}

// dialRingForMembers is dialRingForVerbUser for a verb that reads every member of a cluster
// (dump, restore): each member has its own certificate, so -fingerprint may be given once per
// member, and the dial options come back so the other members are dialled with the same user,
// password and trust. The first connection still has to match one of the pins.
func dialRingForMembers(ctx context.Context, stderr io.Writer, fs *flag.FlagSet, connection *connectionFlags) (*ringclient.Client, string, ringclient.Options, ringRedial, int) {
	return dialRingVerb(ctx, stderr, fs, connection, true)
}

// dialRingVerb is the one dial of the standalone verbs; manyPins accepts -fingerprint more than
// once.
func dialRingVerb(ctx context.Context, stderr io.Writer, fs *flag.FlagSet, connection *connectionFlags, manyPins bool) (*ringclient.Client, string, ringclient.Options, ringRedial, int) {
	spec := connection.spec(flagWasProvided(fs, "address"))
	options, err := resolveConnection(spec)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, terminalSafeText(err.Error()))
		return nil, "", ringclient.Options{}, nil, classify(err).Exit
	}
	resolved := NewCLI(options...)
	if resolved.username == "" || resolved.password == "" {
		_, _ = fmt.Fprintln(stderr, terminalSafeText(errNoCredentials().Error()))
		return nil, "", ringclient.Options{}, nil, ExitUsage
	}
	dial := ringclient.Options{
		Addr: resolved.addr, User: resolved.username, Password: resolved.password,
		Insecure: *connection.insecure,
	}
	if manyPins {
		dial.Fingerprints = append([]string(nil), *connection.fingerprints...)
	} else {
		fingerprint, ok := ringFingerprint(stderr, *connection.fingerprints)
		if !ok {
			return nil, "", ringclient.Options{}, nil, ExitUsage
		}
		dial.Fingerprint = fingerprint
	}
	if dial.Fingerprint == "" && len(dial.Fingerprints) == 0 && !dial.Insecure {
		_, _ = fmt.Fprintln(stderr, ringTrustRefusal)
		return nil, "", ringclient.Options{}, nil, ExitConnect
	}
	// A verb's stdout is its report, so both dial streams are stderr.
	client, exit := dialRingTrusted(ctx, stderr, stderr, dial)
	if client == nil {
		return nil, "", ringclient.Options{}, nil, exit
	}
	return client, resolved.username, dial, func(ctx context.Context) (*ringclient.Client, error) { return redialRing(ctx, dial) }, ExitOK
}

// redialRing opens a second connection with the options of the first. It does
// not print: the sender that resends a frame owns the message and the exit code
// of a redial that fails too (outcome unknown, exit 8).
func redialRing(ctx context.Context, dial ringclient.Options) (*ringclient.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, ringDialTimeout)
	defer cancel()
	client, err := ringclient.Dial(dialCtx, dial)
	if err != nil {
		return nil, fmt.Errorf("re-dialing %s: %w", dial.Addr, err)
	}
	return client, nil
}
