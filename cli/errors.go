package cli

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The CLI's failures are typed so a caller can tell them apart with errors.As
// instead of reading the message: `classify` turns each into a class, a code
// and an exit number (the gap24-one-shot-spec record §6). The rendered text
// of each type is what the REPL printed before the types existed; the REPL's
// output is an API and does not change with them.

// ServerError is an OpError frame: the server answered and refused, and named
// a wire code while doing it.
type ServerError struct {
	// Code is the wire code. It is 0 when Raw is set, because no code arrived.
	Code transport.ErrorCode
	// Message is the server's message, verbatim.
	Message string
	// MaxResultBytes and RetainedBytes accompany RESULT_TOO_LARGE: the node's
	// ceiling and the bytes the query held when it stopped. Zero when absent.
	MaxResultBytes int64
	RetainedBytes  int64
	// Raw is set when the frame's payload could not be decoded, so nothing is
	// known about the failure beyond the fact of it.
	Raw bool
}

// Error renders exactly what the REPL printed before ServerError existed:
// "server error (<code>): <message>", or "server error" when the reply carried
// no message to show.
func (e *ServerError) Error() string {
	if e.Raw || e.Message == "" {
		return "server error"
	}
	return fmt.Sprintf("server error (%d): %s", e.Code, terminalSafeText(e.Message))
}

// RefusedError is an in-band refusal: the server answered a request with a
// success frame whose Success is false, and the frame carries no code.
type RefusedError struct {
	Message string
}

func (e *RefusedError) Error() string { return terminalSafeText(e.Message) }

// NotFoundError is a container the statement named that does not exist: a
// bucket, a database, or a field store. The CLI composes it after a round
// trip that showed the container absent; a missing bucket the server reports
// itself arrives as a ServerError with the NOT_FOUND code instead.
type NotFoundError struct {
	// Kind is "bucket", "database" or "field".
	Kind    string
	Message string
}

func (e *NotFoundError) Error() string { return terminalSafeText(e.Message) }

// UsageError is a request the CLI refused to send: a wrong argument count, an
// operation the bucket's type does not have, a statement that does not parse.
// No write was sent.
type UsageError struct {
	Message string
	// Parse is set when the statement did not parse, so classify can name it
	// PARSE_ERROR and tell a script "fix the statement" from "fix the request".
	Parse bool
}

func (e *UsageError) Error() string { return terminalSafeText(e.Message) }

// ConnectError is a failure to establish a session. Nothing was sent.
type ConnectError struct {
	// Phase is "dial", "tls", "trust", "pin" or "auth-handshake".
	Phase string
	// Address is the server the CLI was reaching. When set, a dial or TLS
	// failure reads as the REPL has always printed it, which
	// the "hql-spec" user-docs page quotes.
	Address string
	Err     error
}

func (e *ConnectError) Error() string {
	switch {
	case e.Address != "" && e.Phase == "dial":
		return fmt.Sprintf("connect to %s: %s", e.Address, terminalSafeText(e.Err.Error()))
	case e.Address != "" && e.Phase == "tls":
		return fmt.Sprintf("build TLS config for %s: %s", e.Address, terminalSafeText(e.Err.Error()))
	}
	return fmt.Sprintf("could not connect (%s): %s", e.Phase, terminalSafeText(e.Err.Error()))
}

func (e *ConnectError) Unwrap() error { return e.Err }

// JSONUnsupportedError is a statement that has no --json form yet.
type JSONUnsupportedError struct {
	// Verb is the statement's route, as the user typed its verb.
	Verb string
}

func (e *JSONUnsupportedError) Error() string {
	return fmt.Sprintf("--json does not support %q yet; run it without --json", e.Verb)
}

// InterruptedError is a run stopped between statements at the caller's request.
type InterruptedError struct {
	// After is how many statements completed; Total is how many were given.
	After, Total int
}

func (e *InterruptedError) Error() string {
	return fmt.Sprintf("interrupted after %d of %d statements", e.After, e.Total)
}

// refusedf builds a *RefusedError, for an in-band refusal that a handler
// words with context ("creating user %q: <the server's message>").
func refusedf(format string, args ...interface{}) error {
	return &RefusedError{Message: fmt.Sprintf(format, args...)}
}

// notFoundf builds a *NotFoundError for a container the statement named.
func notFoundf(kind, format string, args ...interface{}) error {
	return &NotFoundError{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// usagef builds a *UsageError from a format. It takes %v, not %w: a usage
// error is the CLI's own verdict on a request, and the parser's or the
// standard library's error inside it is text for the reader, not a type for a
// caller to match.
func usagef(format string, args ...interface{}) error {
	return &UsageError{Message: fmt.Sprintf(format, args...)}
}
