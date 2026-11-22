package cli

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/hoarddb/hoarddb-server/transport"
)

// errorInfo is what a failure means to a script: a class, a stable code, and
// the exit number, per the gap24-one-shot-spec record §6.
type errorInfo struct {
	// Class matches Exit one to one ("usage", "refused", ...): see ExitClass.
	Class string
	// Code is the stable screaming-snake-case name a script branches on.
	Code string
	Exit int
	// WireCode is the number the server sent, when it sent one.
	WireCode *int
	// MaxResultBytes and RetainedBytes accompany RESULT_TOO_LARGE.
	MaxResultBytes *int64
	RetainedBytes  *int64
	// Message is the server's message verbatim where there is one, otherwise
	// the error's own text.
	Message string
}

// wireCodeRow is one wire code the CLI knows: its name and the exit number
// (hence class) of a server that sends it.
type wireCodeRow struct {
	Code transport.ErrorCode
	Name string
	Exit int
}

// wireCodeTable is the only place a wire code is given a name and a class.
//
// To teach the CLI a code the server has just started sending (the replication
// design adds ones for ownership and epoch refusals), add a row here and a row
// to wireCodeExpectations in classify_test.go; TestErrorCodeMappingIsTotal
// fails until both exist. A field the server adds beside the code (the design's
// write_outcome) is read in classifyServer, the one function that turns a
// *ServerError into an errorInfo, and decoded in CommandHandler.send.
//
// A code that is not in this table is not an error in the CLI: it renders as
// UNKNOWN_<n>, exits 2, and never as success.
//
// INTERNAL is exit 2, not 8: the server gives INTERNAL to every error without
// a sentinel, including a missing database and an authorization refusal
// , so treating it as transient would have a retry loop retry a
// misspelled database name forever. Exit 8 is reserved for the codes the
// server assigns deliberately: UNAVAILABLE, TIMEOUT, EARLY_DATA_REJECTED and
// OVERLOADED.
var wireCodeTable = [...]wireCodeRow{
	{transport.ErrNotFound, "NOT_FOUND", ExitRefused},
	{transport.ErrAlreadyExists, "ALREADY_EXISTS", ExitRefused},
	{transport.ErrPermissionDenied, "PERMISSION_DENIED", ExitRefused},
	{transport.ErrInvalidArgument, "INVALID_ARGUMENT", ExitRefused},
	{transport.ErrInternal, "INTERNAL", ExitRefused},
	{transport.ErrUnavailable, "UNAVAILABLE", ExitUnavailable},
	{transport.ErrTimeout, "TIMEOUT", ExitUnavailable},
	{transport.ErrConflict, "CONFLICT", ExitRefused},
	{transport.ErrQuotaExceeded, "QUOTA_EXCEEDED", ExitRefused},
	{transport.ErrUnsupportedVersion, "UNSUPPORTED_VERSION", ExitRefused},
	{transport.ErrEarlyDataRejected, "EARLY_DATA_REJECTED", ExitUnavailable},
	{transport.ErrNotAuthenticated, "NOT_AUTHENTICATED", ExitRefused},
	{transport.ErrOverloaded, "OVERLOADED", ExitUnavailable},
	{transport.ErrResultTooLarge, "RESULT_TOO_LARGE", ExitRefused},
	{transport.ErrDuplicateKey, "DUPLICATE_KEY", ExitRefused},
	{transport.ErrFieldType, "FIELD_TYPE", ExitRefused},
	{transport.ErrBatchTooLarge, "BATCH_TOO_LARGE", ExitRefused},
	{transport.ErrDocumentExists, "DOCUMENT_EXISTS", ExitRefused},
}

// The highest AuthFailReason the CLI names. A higher one renders as
// AUTH_UNKNOWN_<n>; TestErrorCodeMappingIsTotal fails when the transport names
// a ninth, so this constant cannot fall behind unnoticed.
const lastKnownAuthReason = transport.AuthFailInternal

// codeName is the CLI's own name for a wire code. It does not use
// transport.ErrorCode.String, which renders an unknown value with parentheses.
func codeName(code transport.ErrorCode) string {
	for _, row := range wireCodeTable {
		if row.Code == code {
			return row.Name
		}
	}
	return fmt.Sprintf("UNKNOWN_%d", uint8(code))
}

// authCodeName is the code of an OpAuthFail: the reason's name behind an AUTH_
// prefix, because both vocabularies contain INTERNAL and a script must not
// mistake an authentication failure for a server one.
func authCodeName(reason transport.AuthFailReason) string {
	if reason < transport.AuthFailCredentials || reason > lastKnownAuthReason {
		return fmt.Sprintf("AUTH_UNKNOWN_%d", uint8(reason))
	}
	return "AUTH_" + reason.String()
}

// classify maps any error the CLI can produce to its class, code and exit
// number. It matches on types with errors.As and never on message text, so an
// unchanged server wording cannot change a classification, and a server that
// starts sending a more specific code makes the same statement classify more
// specifically with no change here.
func classify(err error) errorInfo {
	var jsonUnsupported *JSONUnsupportedError
	var usage *UsageError
	var notFound *NotFoundError
	var connect *ConnectError
	var auth *AuthError
	var serverErr *ServerError
	var refused *RefusedError
	var interrupted *InterruptedError
	var networkErr net.Error
	var batchOutcome *BatchOutcomeError

	switch {
	case errors.As(err, &batchOutcome):
		if batchOutcome.Cause != nil {
			info := classify(batchOutcome.Cause)
			info.Class, _ = ExitClass(batchOutcome.Exit)
			info.Exit = batchOutcome.Exit
			return info
		}
		return describe(batchOutcome.Exit, "BATCH_PARTIAL", err)
	case errors.As(err, &jsonUnsupported):
		return describe(ExitUsage, "JSON_UNSUPPORTED", err)
	case errors.As(err, &usage):
		if usage.Parse {
			return describe(ExitUsage, "PARSE_ERROR", err)
		}
		return describe(ExitUsage, "USAGE", err)
	case errors.As(err, &notFound):
		return describe(ExitRefused, "NOT_FOUND", err)
	case errors.As(err, &connect):
		return classifyConnect(connect, err)
	case errors.Is(err, ErrNotConnected):
		return describe(ExitConnect, "CONNECT_FAILED", err)
	case errors.As(err, &auth):
		return describe(ExitAuth, authCodeName(auth.Reason), err)
	case errors.As(err, &serverErr):
		return classifyServer(serverErr)
	case errors.As(err, &refused):
		return describe(ExitRefused, "REFUSED", err)
	case errors.As(err, &interrupted):
		return describe(ExitInterrupted, "INTERRUPTED", err)
	case errors.As(err, &networkErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// The request went out and the reply never came back: whether it was
		// applied is unknown, which is what exit 8 says.
		return describe(ExitUnavailable, "CONNECTION_LOST", err)
	}
	return describe(ExitRefused, "UNCLASSIFIED", err)
}

// describe builds an errorInfo whose class is the exit number's own.
func describe(exit int, code string, err error) errorInfo {
	class, _ := ExitClass(exit)
	return errorInfo{Class: class, Code: code, Exit: exit, Message: err.Error()}
}

// classifyServer classifies an OpError by the code the server sent.
func classifyServer(serverErr *ServerError) errorInfo {
	exit := ExitRefused
	for _, row := range wireCodeTable {
		if row.Code == serverErr.Code {
			exit = row.Exit
		}
	}
	info := describe(exit, codeName(serverErr.Code), serverErr)
	if serverErr.Message != "" {
		info.Message = serverErr.Message
	}
	if serverErr.Raw {
		return info
	}
	wireCode := int(serverErr.Code)
	info.WireCode = &wireCode
	if serverErr.Code == transport.ErrResultTooLarge {
		info.MaxResultBytes = &serverErr.MaxResultBytes
		info.RetainedBytes = &serverErr.RetainedBytes
	}
	return info
}

// classifyConnect classifies a failure to establish a session. Every phase is
// exit 6; the code says which. An error the server sent during the AUTH
// handshake (OVERLOADED, at the connection limit) keeps its own name, because
// it is still a connect failure and not an authentication failure.
func classifyConnect(connect *ConnectError, err error) errorInfo {
	code := "CONNECT_FAILED"
	switch connect.Phase {
	case "trust", "pin":
		code = "TLS_UNTRUSTED"
	case "auth-handshake":
		var serverErr *ServerError
		if errors.As(connect.Err, &serverErr) {
			info := classifyServer(serverErr)
			info.Class, _ = ExitClass(ExitConnect)
			info.Exit = ExitConnect
			return info
		}
	}
	return describe(ExitConnect, code, err)
}
